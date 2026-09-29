package client

// RECORDS file tailer (WS2 bootstrap). Reads a recorder file and its rotated
// siblings, survives logrotate, never emits a partial line, and holds nothing
// in memory beyond the output channel (disk is the queue).
//
// Naming: localhost -> swss.rec / sairedis.rec. asicN -> swss.asicN.rec ...
// Rotated: <live>.1 (uncompressed, delaycompress) then <live>.2.gz, .3.gz ...

import (
	"bufio"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"syscall"
	"time"

	log "github.com/golang/glog"
)

const tailPollInterval = 200 * time.Millisecond
const readBufSize = 1 << 20 // 1 MiB

type fileTailer struct {
	dir    string
	source string // "swss" | "sairedis"
	base   string // e.g. "swss.rec" or "swss.asic0.rec"
	loc    *time.Location

	// onLive fires once when the tailer first drains the live file to EOF,
	// i.e. the boundary between replayed history and the live tail.
	onLive   func()
	firstEOF bool
}

func newFileTailer(dir, source, namespace string, loc *time.Location) *fileTailer {
	return &fileTailer{
		dir:    dir,
		source: source,
		base:   liveFileName(source, namespace),
		loc:    loc,
	}
}

// liveFileName maps (source, namespace) to the live file name.
func liveFileName(source, namespace string) string {
	if namespace == "" || namespace == "localhost" {
		return source + ".rec"
	}
	return source + "." + namespace + ".rec" // swss.asic0.rec
}

func (t *fileTailer) livePath() string { return filepath.Join(t.dir, t.base) }

// Run replays from `from` across rotated files (mtime-pruned) then tails live.
func (t *fileTailer) Run(ctx context.Context, from time.Time, out chan<- RawLine) error {
	if !from.IsZero() {
		for _, path := range t.rotatedFiles(from) {
			if err := ctx.Err(); err != nil {
				return err
			}
			if err := t.replayFile(ctx, path, out); err != nil {
				log.V(1).Infof("records: replay %s: %v", path, err)
			}
		}
	}
	return t.tailLive(ctx, from, out)
}

// rotatedFiles returns rotated siblings oldest -> newest, dropping any whose
// mtime is older than `from`. The live file is tailed separately.
func (t *fileTailer) rotatedFiles(from time.Time) []string {
	entries, err := os.ReadDir(t.dir)
	if err != nil {
		return nil
	}
	type rf struct {
		path string
		n    int
	}
	var files []rf
	prefix := t.base + "."
	for _, e := range entries {
		name := e.Name()
		if !strings.HasPrefix(name, prefix) {
			continue
		}
		suf := strings.TrimPrefix(name, prefix) // "1" or "2.gz"
		suf = strings.TrimSuffix(suf, ".gz")    // "1" or "2"
		n, err := strconv.Atoi(suf)
		if err != nil {
			continue
		}
		full := filepath.Join(t.dir, name)
		if !from.IsZero() {
			if fi, err := os.Stat(full); err == nil && fi.ModTime().Before(from) {
				log.V(2).Infof("records: skip %s (mtime %v < from %v)", name, fi.ModTime(), from)
				continue
			}
		}
		files = append(files, rf{full, n})
	}
	// Oldest first == largest rotation index first (.5.gz older than .1).
	sort.Slice(files, func(i, j int) bool { return files[i].n > files[j].n })
	out := make([]string, len(files))
	for i, f := range files {
		out[i] = f.path
	}
	return out
}

// replayFile streams every line of a (possibly gzipped) rotated file.
func (t *fileTailer) replayFile(ctx context.Context, path string, out chan<- RawLine) error {
	f, err := os.Open(path)
	if err != nil {
		return err
	}
	defer f.Close()

	var reader io.Reader = f
	if strings.HasSuffix(path, ".gz") {
		gz, err := gzip.NewReader(f)
		if err != nil {
			return err
		}
		defer gz.Close()
		reader = gz
	}
	inode := inodeOf(f)
	br := bufio.NewReaderSize(reader, readBufSize)
	var offset int64
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		line, err := br.ReadString('\n') // grows for long bulk lines
		offset += int64(len(line))
		if len(line) > 0 && strings.HasSuffix(line, "\n") {
			select {
			case out <- RawLine{Source: t.source, Seq: seqToken(t.source, inode, offset), Line: line}:
			case <-ctx.Done():
				return ctx.Err()
			}
		}
		if err != nil {
			if err == io.EOF {
				return nil
			}
			return err
		}
	}
}

// tailLive reads the live file (from offset 0 when replaying history, or from
// EOF for live-only) and keeps polling, surviving rotation and truncation.
func (t *fileTailer) tailLive(ctx context.Context, from time.Time, out chan<- RawLine) error {
	var (
		f       *os.File
		br      *bufio.Reader
		inode   uint64
		offset  int64
		partial string
		err     error
	)

	openLive := func(seekEnd bool) error {
		f, err = os.Open(t.livePath())
		if err != nil {
			return err
		}
		inode = inodeOf(f)
		offset = 0
		if seekEnd {
			if fi, err2 := f.Stat(); err2 == nil {
				offset = fi.Size()
				f.Seek(offset, io.SeekStart)
			}
		}
		br = bufio.NewReaderSize(f, readBufSize)
		partial = ""
		return nil
	}

	// live-only starts at EOF; replay continues from the start of the live file.
	for {
		if err = openLive(from.IsZero()); err == nil {
			break
		}
		log.V(1).Infof("records: waiting for %s: %v", t.livePath(), err)
		select {
		case <-time.After(time.Second):
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	defer func() {
		if f != nil {
			f.Close()
		}
	}()

	ticker := time.NewTicker(tailPollInterval)
	defer ticker.Stop()

	for {
		// Drain to EOF.
		for {
			line, rerr := br.ReadString('\n')
			if len(line) > 0 {
				if strings.HasSuffix(line, "\n") {
					full := partial + line
					partial = ""
					offset += int64(len(line))
					select {
					case out <- RawLine{Source: t.source, Seq: seqToken(t.source, inode, offset), Line: full}:
					case <-ctx.Done():
						return ctx.Err()
					}
				} else {
					partial += line // incomplete last line; retry next poll
				}
			}
			if rerr != nil {
				break // EOF (or error): stop draining, go poll
			}
		}

		// First time we reach EOF, we have replayed all existing content and
		// are now genuinely live.
		if !t.firstEOF {
			t.firstEOF = true
			if t.onLive != nil {
				t.onLive()
			}
		}

		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-ticker.C:
		}

		// Rotation / truncation check.
		fi, statErr := os.Stat(t.livePath())
		if statErr != nil {
			continue // file briefly gone during rotate; retry
		}
		if newInode := inodeOfPath(t.livePath()); newInode != 0 && newInode != inode {
			// Rotated: drain current fd fully, then switch to the new file at 0.
			t.drain(ctx, br, &partial, &offset, inode, out)
			f.Close()
			if err = openLive(false); err != nil {
				return err
			}
			log.V(2).Infof("records: %s rotated, reopened new inode", t.base)
			continue
		}
		if fi.Size() < offset {
			// Truncated in place: reopen at 0 and note the gap.
			log.V(1).Infof("records: %s truncated (size %d < offset %d)", t.base, fi.Size(), offset)
			f.Close()
			if err = openLive(false); err != nil {
				return err
			}
		}
	}
}

// drain reads any remaining complete lines from the old fd after a rotation.
func (t *fileTailer) drain(ctx context.Context, br *bufio.Reader, partial *string, offset *int64, inode uint64, out chan<- RawLine) {
	for {
		line, rerr := br.ReadString('\n')
		if len(line) > 0 && strings.HasSuffix(line, "\n") {
			full := *partial + line
			*partial = ""
			*offset += int64(len(line))
			select {
			case out <- RawLine{Source: t.source, Seq: seqToken(t.source, inode, *offset), Line: full}:
			case <-ctx.Done():
				return
			}
		}
		if rerr != nil {
			return
		}
	}
}

func seqToken(source string, inode uint64, offset int64) string {
	return fmt.Sprintf("%s:%d:%d", source, inode, offset)
}

func inodeOf(f *os.File) uint64 {
	fi, err := f.Stat()
	if err != nil {
		return 0
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Ino)
	}
	return 0
}

func inodeOfPath(path string) uint64 {
	fi, err := os.Stat(path)
	if err != nil {
		return 0
	}
	if st, ok := fi.Sys().(*syscall.Stat_t); ok {
		return uint64(st.Ino)
	}
	return 0
}
