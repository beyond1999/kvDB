// mid-kv: A Bitcask-inspired key-value store (middle complexity)
// Features:
// - Append-only log segments with per-record CRC32
// - In-memory index (key -> latest record location)
// - Segment rotation by size; read-only cold segments
// - Background compaction/merge to drop tombstones & stale versions
// - Simple TTL (optional) stored in record header (0 = no ttl)
// - fsync policy on Close() and optional SyncEvery ops
//
// This is a single-package demo for clarity. For production, split into subpackages.
// go run . to try; see main() for a quick demo.

package main

import (
	"bufio"
	"bytes"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ---------------------------
// Record + encoding
// ---------------------------
// On-disk layout (little endian):
// magic(2) = 0xB1 0x7C
// version(1) = 1
// flags(1): bit0 tombstone, bit1 hasTTL
// ttlUnix(8): only if hasTTL else omitted
// keyLen(4)
// valLen(4)
// key(bytes)
// val(bytes)
// crc32(4) of everything from magic..val(bytes)

var (
	magic     = []byte{0xB1, 0x7C}
	recV1     = byte(1)
	crcTable  = crc32.MakeTable(crc32.Castagnoli)
	errBadCRC = errors.New("crc mismatch")
)

type recHeader struct {
	Tombstone bool
	TTLUnix   int64 // 0 means no ttl
}

func encodeRecord(h recHeader, key, val []byte) ([]byte, error) {
	var buf bytes.Buffer
	w := bufio.NewWriter(&buf)
	w.Write(magic)
	w.WriteByte(recV1)
	var flags byte
	if h.Tombstone {
		flags |= 1
	}
	hasTTL := h.TTLUnix != 0
	if hasTTL {
		flags |= 1 << 1
	}
	w.WriteByte(flags)
	if hasTTL {
		binary.Write(w, binary.LittleEndian, h.TTLUnix)
	}
	binary.Write(w, binary.LittleEndian, uint32(len(key)))
	binary.Write(w, binary.LittleEndian, uint32(len(val)))
	w.Write(key)
	w.Write(val)
	w.Flush()
	payload := buf.Bytes()
	crc := crc32.Checksum(payload, crcTable)
	var out bytes.Buffer
	out.Write(payload)
	binary.Write(&out, binary.LittleEndian, crc)
	return out.Bytes(), nil
}

type decodedRecord struct {
	recHeader
	Key  []byte
	Val  []byte
	Size int64 // total bytes including crc
}

func decodeRecord(r io.Reader) (*decodedRecord, error) {
	start := &bytes.Buffer{}
	tee := io.TeeReader(r, start)
	// read magic(2) + version(1) + flags(1)
	head := make([]byte, 4)
	if _, err := io.ReadFull(tee, head); err != nil {
		return nil, err
	}
	if !bytes.Equal(head[:2], magic) {
		return nil, io.ErrUnexpectedEOF
	}
	ver := head[2]
	if ver != recV1 {
		return nil, fmt.Errorf("unknown version %d", ver)
	}
	flags := head[3]
	hasTTL := (flags & (1 << 1)) != 0
	var ttl int64
	if hasTTL {
		if err := binary.Read(tee, binary.LittleEndian, &ttl); err != nil {
			return nil, err
		}
	}
	var keyLen, valLen uint32
	if err := binary.Read(tee, binary.LittleEndian, &keyLen); err != nil {
		return nil, err
	}
	if err := binary.Read(tee, binary.LittleEndian, &valLen); err != nil {
		return nil, err
	}
	key := make([]byte, keyLen)
	if _, err := io.ReadFull(tee, key); err != nil {
		return nil, err
	}
	val := make([]byte, valLen)
	if _, err := io.ReadFull(tee, val); err != nil {
		return nil, err
	}
	payload := start.Bytes()
	// read crc
	var crc uint32
	if err := binary.Read(r, binary.LittleEndian, &crc); err != nil {
		return nil, err
	}
	calc := crc32.Checksum(payload, crcTable)
	if calc != crc {
		return nil, errBadCRC
	}
	return &decodedRecord{
		recHeader: recHeader{Tombstone: (flags & 1) != 0, TTLUnix: ttl},
		Key:       key,
		Val:       val,
		Size:      int64(len(payload)) + 4,
	}, nil
}

// ---------------------------
// Segment
// ---------------------------

type segment struct {
	id     int64
	path   string
	file   *os.File
	w      *bufio.Writer
	read   *os.File // for random reads
	size   int64
	ro     bool
	closed bool
}

func openSegment(dir string, id int64, readonly bool) (*segment, error) {
	name := fmt.Sprintf("%016x.seg", id)
	path := filepath.Join(dir, name)
	var f *os.File
	var err error
	if readonly {
		f, err = os.Open(path)
	} else {
		f, err = os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0644)
	}
	if err != nil {
		return nil, err
	}
	info, _ := f.Stat()
	s := &segment{id: id, path: path, size: info.Size(), ro: readonly}
	if readonly {
		s.read = f
	} else {
		s.file = f
		s.w = bufio.NewWriterSize(f, 1<<20)
		// separate reader for random-read
		rf, err := os.Open(path)
		if err != nil {
			f.Close()
			return nil, err
		}
		s.read = rf
	}
	return s, nil
}

func (s *segment) append(rec []byte, sync bool) (off int64, n int, err error) {
	if s.ro {
		return 0, 0, errors.New("append on read-only segment")
	}
	off = s.size
	n, err = s.w.Write(rec)
	if err != nil {
		return
	}
	s.size += int64(n)
	if sync {
		s.w.Flush()
		s.file.Sync()
	}
	return
}

func (s *segment) readAt(off int64) (*decodedRecord, error) {
	if _, err := s.read.Seek(off, io.SeekStart); err != nil {
		return nil, err
	}
	return decodeRecord(s.read)
}

func (s *segment) flushSync() error {
	if s.ro || s.file == nil {
		return nil
	}
	if err := s.w.Flush(); err != nil {
		return err
	}
	return s.file.Sync()
}

func (s *segment) close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	var err1, err2 error
	if s.w != nil {
		_ = s.w.Flush()
	}
	if s.file != nil {
		err1 = s.file.Close()
	}
	if s.read != nil {
		err2 = s.read.Close()
	}
	if err1 != nil {
		return err1
	}
	return err2
}

// ---------------------------
// Index + Engine
// ---------------------------

type location struct {
	segID int64
	off   int64
	size  int64
	ttl   int64
	dead  bool // tombstone
}

type Options struct {
	Dir             string
	MaxSegmentSize  int64 // bytes (e.g., 64MB)
	SyncEvery       int   // fsync every N writes; 0 = only on Close
	CompactTriggerN int   // if cold segments >= N, trigger compaction
}

type Engine struct {
	mu     sync.RWMutex
	opts   Options
	active *segment
	cold   []*segment // read-only historical segments newest last
	index  map[string]location
	writes int
	stopCh chan struct{}
	wg     sync.WaitGroup
}

func Open(opts Options) (*Engine, error) {
	if opts.Dir == "" {
		return nil, errors.New("Dir required")
	}
	if opts.MaxSegmentSize == 0 {
		opts.MaxSegmentSize = 64 << 20
	}
	if opts.CompactTriggerN == 0 {
		opts.CompactTriggerN = 4
	}
	if err := os.MkdirAll(opts.Dir, 0755); err != nil {
		return nil, err
	}
	// list existing segments
	entries, _ := os.ReadDir(opts.Dir)
	var ids []int64
	for _, e := range entries {
		name := e.Name()
		if strings.HasSuffix(name, ".seg") {
			var id int64
			fmt.Sscanf(strings.TrimSuffix(name, ".seg"), "%x", &id)
			ids = append(ids, id)
		}
	}
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	e := &Engine{opts: opts, index: make(map[string]location), stopCh: make(chan struct{})}
	// open cold segments and rebuild index by scanning from oldest to newest
	for i, id := range ids {
		ro := true
		seg, err := openSegment(opts.Dir, id, ro)
		if err != nil {
			return nil, err
		}
		if i == len(ids)-1 { // last one becomes active (writable)
			seg.close()
			seg, err = openSegment(opts.Dir, id, false)
			if err != nil {
				return nil, err
			}
			e.active = seg
		} else {
			e.cold = append(e.cold, seg)
		}
		// scan to rebuild index
		var off int64
		for off < seg.size {
			rec, err := seg.readAt(off)
			if err != nil {
				break
			}
			key := string(rec.Key)
			loc := location{segID: seg.id, off: off, size: rec.Size, ttl: rec.TTLUnix, dead: rec.Tombstone}
			e.index[key] = loc
			off += rec.Size
		}
	}
	if e.active == nil { // fresh start
		seg, err := openSegment(opts.Dir, time.Now().UnixNano(), false)
		if err != nil {
			return nil, err
		}
		e.active = seg
	}
	// background compactor
	e.wg.Add(1)
	go e.compactor()
	return e, nil
}

func (e *Engine) rotateLocked() error {
	// make current active read-only cold; open new active
	old := e.active
	old.flushSync()
	old.close()
	// reopen as read-only
	ro, err := openSegment(e.opts.Dir, old.id, true)
	if err != nil {
		return err
	}
	e.cold = append(e.cold, ro)
	// new active
	seg, err := openSegment(e.opts.Dir, time.Now().UnixNano(), false)
	if err != nil {
		return err
	}
	e.active = seg
	return nil
}

func (e *Engine) shouldExpire(loc location) bool {
	if loc.ttl == 0 {
		return false
	}
	return time.Now().Unix() >= loc.ttl
}

func (e *Engine) Set(key string, val []byte, ttl time.Duration) error {
	var ttlUnix int64
	if ttl > 0 {
		ttlUnix = time.Now().Add(ttl).Unix()
	}
	rec, _ := encodeRecord(recHeader{TTLUnix: ttlUnix}, []byte(key), val)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active.size+int64(len(rec)) > e.opts.MaxSegmentSize {
		if err := e.rotateLocked(); err != nil {
			return err
		}
	}
	off, n, err := e.active.append(rec, false)
	if err != nil {
		return err
	}
	e.index[key] = location{segID: e.active.id, off: off, size: int64(n), ttl: ttlUnix, dead: false}
	e.writes++
	if e.opts.SyncEvery > 0 && e.writes%e.opts.SyncEvery == 0 {
		_ = e.active.flushSync()
	}
	return nil
}

func (e *Engine) Get(key string) ([]byte, bool, error) {
	e.mu.RLock()
	loc, ok := e.index[key]
	if !ok {
		e.mu.RUnlock()
		return nil, false, nil
	}
	dead := loc.dead || e.shouldExpire(loc)
	seg := e.segmentByIDLocked(loc.segID)
	e.mu.RUnlock()
	if dead || seg == nil {
		return nil, false, nil
	}
	rec, err := seg.readAt(loc.off)
	if err != nil {
		return nil, false, err
	}
	if rec.Tombstone || (rec.TTLUnix != 0 && time.Now().Unix() >= rec.TTLUnix) {
		return nil, false, nil
	}
	return rec.Val, true, nil
}

func (e *Engine) Del(key string) error {
	rec, _ := encodeRecord(recHeader{Tombstone: true}, []byte(key), nil)
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active.size+int64(len(rec)) > e.opts.MaxSegmentSize {
		if err := e.rotateLocked(); err != nil {
			return err
		}
	}
	off, n, err := e.active.append(rec, false)
	if err != nil {
		return err
	}
	e.index[key] = location{segID: e.active.id, off: off, size: int64(n), dead: true}
	e.writes++
	if e.opts.SyncEvery > 0 && e.writes%e.opts.SyncEvery == 0 {
		_ = e.active.flushSync()
	}
	return nil
}

func (e *Engine) segmentByIDLocked(id int64) *segment {
	if e.active != nil && e.active.id == id {
		return e.active
	}
	for _, s := range e.cold {
		if s.id == id {
			return s
		}
	}
	return nil
}

func (e *Engine) Close() error {
	close(e.stopCh)
	e.wg.Wait()
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.active != nil {
		e.active.flushSync()
		e.active.close()
	}
	for _, s := range e.cold {
		s.close()
	}
	return nil
}

// ---------------------------
// Compaction
// ---------------------------
func (e *Engine) compactor() {
	defer e.wg.Done()
	t := time.NewTicker(2 * time.Second)
	for {
		select {
		case <-e.stopCh:
			return
		case <-t.C:
			e.tryCompact()
		}
	}
}

func (e *Engine) tryCompact() {
	e.mu.Lock()
	defer e.mu.Unlock()
	if len(e.cold) < e.opts.CompactTriggerN {
		return
	}
	// heuristic: compact oldest half of cold segments into one new cold segment
	n := int(math.Max(1, float64(len(e.cold))/2))
	toCompact := make([]*segment, n)
	copy(toCompact, e.cold[:n])
	// build a set of live keys we should carry forward
	live := make(map[string]bool)
	for k, loc := range e.index {
		// only if the latest version for k resides in one of toCompact and not expired/tomb
		if loc.dead || e.shouldExpire(loc) {
			continue
		}
		for _, s := range toCompact {
			if s.id == loc.segID {
				live[k] = true
				break
			}
		}
	}
	if len(live) == 0 { // nothing to do, just unlink files
		for _, s := range toCompact {
			s.close()
			os.Remove(s.path)
		}
		e.cold = e.cold[n:]
		return
	}
	// write merged segment as read-only file
	mergeID := time.Now().UnixNano()
	outPath := filepath.Join(e.opts.Dir, fmt.Sprintf("%016x.seg", mergeID))
	out, err := os.OpenFile(outPath, os.O_CREATE|os.O_RDWR, 0644)
	if err != nil {
		return
	}
	ow := bufio.NewWriterSize(out, 1<<20)
	var off int64
	// for deterministic order, iterate keys sorted
	keys := make([]string, 0, len(live))
	for k := range live {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		loc := e.index[k]
		seg := e.segmentByIDLocked(loc.segID)
		if seg == nil {
			continue
		}
		rec, err := seg.readAt(loc.off)
		if err != nil {
			continue
		}
		if rec.Tombstone || (rec.TTLUnix != 0 && time.Now().Unix() >= rec.TTLUnix) {
			continue
		}
		enc, _ := encodeRecord(recHeader{TTLUnix: rec.TTLUnix}, rec.Key, rec.Val)
		if _, err := ow.Write(enc); err != nil {
			continue
		}
		// update index to new location
		e.index[k] = location{segID: mergeID, off: off, size: int64(len(enc)), ttl: rec.TTLUnix, dead: false}
		off += int64(len(enc))
	}
	ow.Flush()
	out.Sync()
	out.Close()
	// remove old compacted segments
	for _, s := range toCompact {
		s.close()
		os.Remove(s.path)
	}
	// insert merged as cold (read-only)
	merged, err := openSegment(e.opts.Dir, mergeID, true)
	if err == nil {
		// drop the first n cold segments and prepend the merged one to keep order roughly chronological
		e.cold = append([]*segment{merged}, e.cold[n:]...)
	}
}

// ---------------------------
// Demo main
// ---------------------------
func main() {
	dir := filepath.Join(os.TempDir(), "mid-kv")
	os.MkdirAll(dir, 0755)
	fmt.Println("DB dir:", dir)
	db, err := Open(Options{Dir: dir, MaxSegmentSize: 1 << 20, SyncEvery: 0, CompactTriggerN: 3})
	if err != nil {
		panic(err)
	}
	defer db.Close()

	// Basic ops
	_ = db.Set("hello", []byte("world"), 0)
	_ = db.Set("foo", []byte("bar"), 2*time.Second)
	val, ok, _ := db.Get("hello")
	fmt.Println("hello?", ok, string(val))
	val, ok, _ = db.Get("foo")
	fmt.Println("foo before ttl?", ok, string(val))
	time.Sleep(2100 * time.Millisecond)
	_, ok, _ = db.Get("foo")
	fmt.Println("foo after ttl?", ok)
	_ = db.Del("hello")
	_, ok, _ = db.Get("hello")
	fmt.Println("hello after del?", ok)

	// generate many writes to force rotation and compaction
	for i := 0; i < 2000; i++ {
		key := fmt.Sprintf("k%05d", i)
		_ = db.Set(key, []byte(strings.Repeat("x", 128)), 0)
	}
	fmt.Println("written 2000 keys; cold segments:", len(db.cold))
}
