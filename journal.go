// Copyright FlowChassis.com 2026
// SPDX-License-Identifier: Apache-2.0

package flowchassis

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// The journal file starts with an 8-byte header, "FLOWCJ" and the format
// version as two digits. Then come records, each framed as a 4-byte
// little-endian payload length, a 4-byte CRC-32C of the payload, and the
// payload, which is one JSON object. Records are only ever appended, and each
// append is flushed to disk before the engine moves on.
const (
	headerMagic = "FLOWCJ"
	frameSize   = 8
	// MaxRecord is the largest record the journal accepts, which bounds the
	// size of a run's state and input.
	MaxRecord = 16 << 20
)

var (
	header = []byte(fmt.Sprintf("%s%02d", headerMagic, JournalFormat))
	crcTab = crc32.MakeTable(crc32.Castagnoli)
)

// Record kinds. Each is one line of a run's history.
const (
	recStart    = "start"    // a run began: flow, input
	recBegin    = "begin"    // an attempt of a step is about to run
	recDone     = "done"     // a step completed: its state and the next step
	recFail     = "fail"     // an attempt failed: retry_at, or final
	recCancel   = "cancel"   // cancellation was asked for
	recComp     = "comp"     // compensation began: why
	recUBegin   = "ubegin"   // an attempt of an undo is about to run
	recUDone    = "udone"    // an undo completed
	recUFail    = "ufail"    // an undo attempt failed: retry_at, or final
	recEnd      = "end"      // the run finished: status
	recSnapshot = "snapshot" // compaction: a whole run in one record
)

// Record is one entry of the journal, as stored. Inspect returns them so
// tools can show a run's history.
type Record struct {
	Type    string            `json:"t"`
	Run     string            `json:"run"`
	At      time.Time         `json:"at"`
	Flow    string            `json:"flow,omitempty"`
	Input   []byte            `json:"input,omitempty"`
	Step    string            `json:"step,omitempty"`
	Seq     int               `json:"seq,omitempty"`
	Attempt int               `json:"attempt,omitempty"`
	State   map[string][]byte `json:"state,omitempty"`
	Next    *string           `json:"next,omitempty"`
	Error   string            `json:"err,omitempty"`
	RetryAt *time.Time        `json:"retry_at,omitempty"`
	Final   bool              `json:"final,omitempty"`
	Status  Status            `json:"status,omitempty"`
	// Snap carries a whole run, in snapshot records written by compaction.
	Snap *snapshot `json:"snap,omitempty"`
}

// Store is where the journal lives. The engine appends one encoded frame at
// a time and must not go on until Append has made it durable. FileStore is
// the store for real use; MemStore keeps the journal in memory.
type Store interface {
	// Load returns the frames written so far, without the header, and makes
	// the store ready for appends after them. It is called once, first.
	Load() ([]byte, error)
	// Append writes frames after the last one and returns once they are
	// durable.
	Append(frames []byte) error
	// Replace swaps the whole journal for the given frames, atomically.
	Replace(frames []byte) error
	// Close releases the store.
	Close() error
}

func encode(r *Record) ([]byte, error) {
	payload, err := json.Marshal(r)
	if err != nil {
		return nil, err
	}
	if len(payload) > MaxRecord {
		return nil, fmt.Errorf("flowchassis: record of %d bytes is over the %d-byte limit", len(payload), MaxRecord)
	}
	frame := make([]byte, frameSize+len(payload))
	binary.LittleEndian.PutUint32(frame[0:4], uint32(len(payload)))
	binary.LittleEndian.PutUint32(frame[4:8], crc32.Checksum(payload, crcTab))
	copy(frame[frameSize:], payload)
	return frame, nil
}

// decodeAll reads frames from b. It returns the records, how many bytes of b
// they cover, and whether what follows is a torn final write. Damage that
// isn't at the very end is an error.
func decodeAll(b []byte) (recs []Record, good int, torn bool, err error) {
	off := 0
	for off < len(b) {
		bad := func() (bool, error) {
			// A torn final write leaves less than a frame header, zeros, or
			// a frame with a plausible length that runs to or past the end
			// of the file. Anything else is damage, and nothing after it is
			// thrown away.
			if off+frameSize > len(b) || allZero(b[off:]) {
				return true, nil
			}
			n := int(binary.LittleEndian.Uint32(b[off : off+4]))
			if n > 0 && n <= MaxRecord && off+frameSize+n >= len(b) {
				return true, nil
			}
			return false, fmt.Errorf("%w: bad record at byte %d of %d", ErrCorrupt, off+len(header), len(b)+len(header))
		}
		if off+frameSize > len(b) {
			t, e := bad()
			return recs, off, t, e
		}
		n := int(binary.LittleEndian.Uint32(b[off : off+4]))
		sum := binary.LittleEndian.Uint32(b[off+4 : off+8])
		if n == 0 || n > MaxRecord || off+frameSize+n > len(b) {
			t, e := bad()
			return recs, off, t, e
		}
		payload := b[off+frameSize : off+frameSize+n]
		if crc32.Checksum(payload, crcTab) != sum {
			t, e := bad()
			return recs, off, t, e
		}
		var r Record
		if err := json.Unmarshal(payload, &r); err != nil || r.Type == "" || r.Run == "" {
			// The checksum matched, so these bytes were written whole: this
			// is not a torn write.
			return recs, off, false, fmt.Errorf("%w: unreadable record at byte %d", ErrCorrupt, off+len(header))
		}
		recs = append(recs, r)
		off += frameSize + n
	}
	return recs, off, false, nil
}

func allZero(b []byte) bool {
	for _, c := range b {
		if c != 0 {
			return false
		}
	}
	return true
}

// FileStore keeps the journal in one file, flushed to disk on every append.
type FileStore struct {
	path string
	f    *os.File
	size int64
}

// NewFileStore returns a store for the journal at path. The file is created
// on Load if it doesn't exist.
func NewFileStore(path string) *FileStore { return &FileStore{path: path} }

// Load reads the journal, cuts off a torn final write if there is one, and
// opens the file for appends.
func (s *FileStore) Load() ([]byte, error) {
	_ = os.Remove(s.path + ".compact")
	f, err := os.OpenFile(s.path, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	data, err := io.ReadAll(f)
	if err != nil {
		f.Close()
		return nil, err
	}
	if len(data) == 0 {
		if _, err := f.Write(header); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
		syncDir(filepath.Dir(s.path))
		s.f, s.size = f, int64(len(header))
		return nil, nil
	}
	if err := checkHeader(data); err != nil {
		f.Close()
		return nil, err
	}
	body := data[len(header):]
	_, good, torn, err := decodeAll(body)
	if err != nil {
		f.Close()
		return nil, err
	}
	if torn {
		if err := f.Truncate(int64(len(header) + good)); err != nil {
			f.Close()
			return nil, err
		}
		if err := f.Sync(); err != nil {
			f.Close()
			return nil, err
		}
	}
	s.f, s.size = f, int64(len(header)+good)
	return body[:good], nil
}

func checkHeader(data []byte) error {
	if len(data) < len(header) {
		if bytes.HasPrefix(header, data) {
			return fmt.Errorf("%w: the header is cut short", ErrCorrupt)
		}
		return fmt.Errorf("%w: not a FlowChassis journal", ErrCorrupt)
	}
	if !bytes.HasPrefix(data, []byte(headerMagic)) {
		return fmt.Errorf("%w: not a FlowChassis journal", ErrCorrupt)
	}
	if !bytes.Equal(data[:len(header)], header) {
		return fmt.Errorf("flowchassis: journal format %q isn't supported by this build, which reads format %02d", data[len(headerMagic):len(header)], JournalFormat)
	}
	return nil
}

// Append writes frames at the end of the file and flushes them to disk.
func (s *FileStore) Append(frames []byte) error {
	if s.f == nil {
		return ErrClosed
	}
	n, err := s.f.WriteAt(frames, s.size)
	if err != nil {
		if n > 0 {
			// Leave no half record behind for the next append to follow.
			_ = s.f.Truncate(s.size)
		}
		return err
	}
	if err := s.f.Sync(); err != nil {
		return err
	}
	s.size += int64(n)
	return nil
}

// Replace writes the frames to a new file, flushes it and moves it over the
// journal, so a crash leaves either the old journal or the new one.
func (s *FileStore) Replace(frames []byte) error {
	tmp := s.path + ".compact"
	f, err := os.OpenFile(tmp, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(append(append([]byte(nil), header...), frames...)); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return err
	}
	// Windows won't move a file that is open, or move one over a file that
	// is open, so both are closed first and the journal opened again after.
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return err
	}
	old := s.f
	s.f = nil
	if old != nil {
		old.Close()
	}
	renameErr := os.Rename(tmp, s.path)
	if renameErr != nil {
		os.Remove(tmp)
	} else {
		syncDir(filepath.Dir(s.path))
	}
	reopened, err := os.OpenFile(s.path, os.O_RDWR, 0o644)
	if err != nil {
		// Appends now fail, which stops the engine.
		return errors.Join(renameErr, err)
	}
	st, err := reopened.Stat()
	if err != nil {
		reopened.Close()
		return errors.Join(renameErr, err)
	}
	s.f, s.size = reopened, st.Size()
	return renameErr
}

// Close closes the file.
func (s *FileStore) Close() error {
	if s.f == nil {
		return nil
	}
	err := s.f.Close()
	s.f = nil
	return err
}

// MemStore keeps the journal in memory. It is for tests and for programs
// that want the engine's retries and compensation without durability.
// Bytes returns what a FileStore would hold, so a test can hand the journal
// to a new engine as if after a restart.
type MemStore struct {
	mu   sync.Mutex
	data []byte
	// FailAppend, when set, is called before each append; an error it
	// returns fails the append. Tests use it to simulate a full disk.
	FailAppend func(frames []byte) error
}

// NewMemStore returns a memory store holding the given frames, such as the
// Bytes of another MemStore.
func NewMemStore(frames []byte) *MemStore {
	return &MemStore{data: append([]byte(nil), frames...)}
}

// Load returns the frames held, cutting off a torn final write.
func (m *MemStore) Load() ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	_, good, _, err := decodeAll(m.data)
	if err != nil {
		return nil, err
	}
	m.data = m.data[:good]
	return append([]byte(nil), m.data...), nil
}

// Append adds frames.
func (m *MemStore) Append(frames []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.FailAppend != nil {
		if err := m.FailAppend(frames); err != nil {
			return err
		}
	}
	m.data = append(m.data, frames...)
	return nil
}

// Replace swaps the frames held.
func (m *MemStore) Replace(frames []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data = append([]byte(nil), frames...)
	return nil
}

// Close does nothing.
func (m *MemStore) Close() error { return nil }

// Bytes returns a copy of the frames held.
func (m *MemStore) Bytes() []byte {
	m.mu.Lock()
	defer m.mu.Unlock()
	return append([]byte(nil), m.data...)
}

// ReadJournal reads a journal file without opening it for writing or taking
// its lock, so it works while an engine has the file open. A torn final
// write, which a running engine can leave for a moment, is left out and
// reported in torn.
func ReadJournal(path string) (recs []Record, torn bool, err error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, false, err
	}
	if len(data) == 0 {
		return nil, false, nil
	}
	if err := checkHeader(data); err != nil {
		return nil, false, err
	}
	recs, _, torn, err = decodeAll(data[len(header):])
	return recs, torn, err
}

var errNoUndo = errors.New("flowchassis: a completed step can't be undone because the flow no longer has it")
