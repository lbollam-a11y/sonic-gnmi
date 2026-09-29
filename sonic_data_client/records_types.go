package client

// RECORDS target: stream orchagent / sairedis recorder files over gNMI.
//
// This file freezes the contracts shared by the RECORDS workstreams (WS1-WS3):
//   - Record         the normalised unit the operator sees
//   - RawLine        one physical line read from a file, before parsing
//   - Tailer         WS2 owns the implementation, WS1 consumes it
//   - Parser/Matcher WS3 owns them, WS1 consumes them
//
// Bootstrap note (WS5): a minimal end-to-end implementation of all three
// interfaces lives alongside this file so the target builds, demos and can be
// swapped out piecemeal as WS1-WS3 land their production versions.

import (
	"context"
	"os"
	"time"
)

// Configuration knobs, read from the environment.
//
// NOTE: telemetry's main() parses its own flags through a private flag.FlagSet
// (see setupFlags in telemetry/telemetry.go), so a package-level flag.String in
// this package is NOT accepted on the telemetry command line — it would be
// rejected as an unknown flag. We therefore drive the RECORDS knobs from env
// vars, which the gnmi container start script exports (WS5 container wiring):
//
//	export RECORDS_DIR=/mnt/host/var/log/swss   # default when unset
//	export RECORDS_TZ=UTC                        # empty = container local time
const defaultRecordsDir = "/mnt/host/var/log/swss"

func recordsDirEnv() string {
	if d := os.Getenv("RECORDS_DIR"); d != "" {
		return d
	}
	return defaultRecordsDir
}

func recordsTzEnv() string { return os.Getenv("RECORDS_TZ") }

// Recorder timestamp layout: localtime, no zone, microsecond precision.
// Example: 2026-09-26.10:15:32.123456
const recordsTSLayout = "2006-01-02.15:04:05.000000"

// Source / DB / op constants.
const (
	srcSwss     = "swss"
	srcSairedis = "sairedis"

	dbAPPL = "APPL_DB"
	dbASIC = "ASIC_DB"

	asicStateTable = "ASIC_STATE" // the only TABLE token for ASIC_DB paths

	saiTypePrefix = "SAI_OBJECT_TYPE_"
	saiStatusOK   = "SAI_STATUS_SUCCESS"
)

// Record is one parsed line from a recorder file, normalised for the operator.
type Record struct {
	Seq     string            // resumable cursor: "<source>:<inode>:<offset>"
	TS      time.Time         // parsed from the localtime stamp in the file
	Source  string            // "swss" | "sairedis"
	DB      string            // "APPL_DB" | "ASIC_DB"
	Table   string            // ROUTE_TABLE, or SAI_OBJECT_TYPE_ROUTE_ENTRY for sairedis
	Key     string            // key as written in the file, after normalisation
	Op      string            // SET/DEL for swss, the sairedis opcode letter otherwise
	Fields  map[string]string // field:value or attr=val pairs
	Status  string            // filled from a following E| line, "" on success
	Matched string            // how the subscription matched: becomes matched_by
	Raw     string            // the original line, for debugging
}

// RawLine is one physical line read from a file, before parsing.
type RawLine struct {
	Source string // "swss" | "sairedis"
	Seq    string // "<source>:<inode>:<offset>"
	Line   string
}

// Tailer replays from `from` (zero time = no replay) across rotated files, then
// tails live until ctx is cancelled. It blocks on `out` when the consumer is
// slow: disk is the queue, we buffer nothing beyond the channel.
type Tailer interface {
	Run(ctx context.Context, from time.Time, out chan<- RawLine) error
}

// Parser turns a RawLine into a Record. ok=false means skip the line.
type Parser interface {
	Parse(l RawLine) (*Record, bool)
}

// Matcher decides whether a subscriber wanted this record. `how` becomes the
// matched_by field in the output.
type Matcher interface {
	Match(r *Record) (ok bool, how string)
}
