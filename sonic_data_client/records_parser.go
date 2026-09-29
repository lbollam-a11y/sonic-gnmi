package client

// RECORDS parsers (WS3 bootstrap). Turns a RawLine into a normalised Record.
//
// swss.rec:      ts|TABLE:key|op|f:v|f:v...
// sairedis.rec:  ts|opcode|<key or api>|attr=val|...   (bulk uses || separators)
//                ts|E|SAI_STATUS_...                   (failure, sync modes only)
//
// Timestamps are localtime with no zone (YYYY-MM-DD.HH:MM:SS.uuuuuu); we parse
// them in the configured timezone.

import (
	"strings"
	"time"
)

type recordsParser struct {
	loc          *time.Location
	lastSairedis *Record // for E| attachment: last emitted sairedis record
}

func newRecordsParser(loc *time.Location) *recordsParser {
	if loc == nil {
		loc = time.Local
	}
	return &recordsParser{loc: loc}
}

func (p *recordsParser) parseTS(s string) (time.Time, error) {
	return time.ParseInLocation(recordsTSLayout, s, p.loc)
}

// Parse dispatches on the line's source.
func (p *recordsParser) Parse(l RawLine) (*Record, bool) {
	switch l.Source {
	case srcSwss:
		return p.parseSwss(l)
	case srcSairedis:
		return p.parseSairedis(l)
	default:
		return nil, false
	}
}

func (p *recordsParser) parseSwss(l RawLine) (*Record, bool) {
	line := strings.TrimRight(l.Line, "\r\n")
	if line == "" || line[0] == '#' {
		return nil, false
	}
	parts := strings.Split(line, "|")
	if len(parts) < 3 {
		return nil, false // banner "ts|recording started" etc.
	}
	ts, err := p.parseTS(parts[0])
	if err != nil {
		return nil, false
	}
	tableKey := parts[1]
	ci := strings.IndexByte(tableKey, ':')
	if ci < 0 {
		return nil, false
	}
	op := strings.ToUpper(parts[2])
	if op != "SET" && op != "DEL" {
		return nil, false // unrecognised APPL_DB op / marker
	}
	rec := &Record{
		Source: srcSwss,
		DB:     dbAPPL,
		Table:  tableKey[:ci],
		Key:    tableKey[ci+1:],
		Op:     op,
		Seq:    l.Seq,
		TS:     ts,
		Raw:    line,
		Fields: map[string]string{},
	}
	for _, f := range parts[3:] {
		if f == "" {
			continue
		}
		if fi := strings.IndexByte(f, ':'); fi >= 0 {
			rec.Fields[f[:fi]] = f[fi+1:]
		} else {
			rec.Fields[f] = ""
		}
	}
	return rec, true
}

func (p *recordsParser) parseSairedis(l RawLine) (*Record, bool) {
	line := strings.TrimRight(l.Line, "\r\n")
	if line == "" || line[0] == '#' {
		return nil, false
	}
	parts := strings.Split(line, "|")
	if len(parts) < 2 {
		return nil, false
	}
	ts, err := p.parseTS(parts[0])
	if err != nil {
		return nil, false
	}
	op := parts[1]

	// E| failure line: attach to the previously emitted sairedis record and
	// re-emit it with Status set. The matcher decides (ops=E) whether the
	// subscriber wanted it.
	if op == "E" {
		if p.lastSairedis == nil {
			return nil, false
		}
		rec := *p.lastSairedis
		rec.Fields = cloneFields(p.lastSairedis.Fields)
		if len(parts) >= 3 && parts[2] != "" {
			rec.Status = parts[2]
		} else {
			rec.Status = "SAI_STATUS_FAILURE"
		}
		rec.Fields["_response"] = "E"
		rec.Seq = l.Seq
		rec.TS = ts
		rec.Raw = line
		return &rec, true
	}

	rec := &Record{
		Source: srcSairedis,
		DB:     dbASIC,
		Op:     op,
		Seq:    l.Seq,
		TS:     ts,
		Raw:    line,
		Fields: map[string]string{},
	}

	rest := parts[2:]
	keyIdx := -1
	for i, t := range rest {
		if strings.HasPrefix(t, saiTypePrefix) {
			keyIdx = i
			break
		}
	}

	if keyIdx == -1 {
		// Response / status line (e.g. Q|attribute_capability|SAI_STATUS_...).
		if len(rest) > 0 {
			rec.Fields["_api"] = rest[0]
			for _, t := range rest[1:] {
				addAttr(rec.Fields, t)
			}
			if strings.HasPrefix(rest[0], "SAI_STATUS_") {
				rec.Status = rest[0]
			}
		}
		p.lastSairedis = rec
		return rec, true
	}

	if keyIdx > 0 {
		rec.Fields["_api"] = strings.Join(rest[:keyIdx], ":")
	}
	keyTok := rest[keyIdx]
	attrs := rest[keyIdx+1:]

	// Bulk: uppercase opcode with a `||` right after the type token, i.e.
	// S|SAI_OBJECT_TYPE_ROUTE_ENTRY||{k1}|a1||{k2}|a2...
	if isUpper(op) && len(attrs) > 0 && attrs[0] == "" {
		rec.Table = keyTok
		var keys []string
		for _, t := range attrs {
			if t == "" {
				continue
			}
			if looksLikeSaiKey(t) {
				keys = append(keys, t)
			} else {
				addAttr(rec.Fields, t)
			}
		}
		if len(keys) > 0 {
			rec.Key = normalizeSaiKey(keys[0])
			rec.Fields["_keys"] = strings.Join(keys, ";")
		}
		p.lastSairedis = rec
		return rec, true
	}

	// Standard single op: split the key token into type + key on the first ':'.
	rec.Table, rec.Key = splitSaiKeyToken(keyTok)
	rec.Key = normalizeSaiKey(rec.Key)
	for _, t := range attrs {
		addAttr(rec.Fields, t)
	}
	p.lastSairedis = rec
	return rec, true
}

// --- helpers ---

func isUpper(s string) bool {
	return s != "" && s == strings.ToUpper(s) && s != strings.ToLower(s)
}

func looksLikeSaiKey(t string) bool {
	return strings.HasPrefix(t, "{") || strings.HasPrefix(t, "oid:") || strings.HasPrefix(t, saiTypePrefix)
}

// splitSaiKeyToken splits "SAI_OBJECT_TYPE_X:<oid or json>" on the first ':'.
func splitSaiKeyToken(tok string) (table, key string) {
	if i := strings.IndexByte(tok, ':'); i >= 0 {
		return tok[:i], tok[i+1:]
	}
	return tok, ""
}

// normalizeSaiKey trims surrounding whitespace. Full JSON canonicalisation
// (field order, IPv6 forms) is a WS3 hardening item; correlation uses the
// extracted dest/ip rather than raw key equality.
func normalizeSaiKey(k string) string {
	return strings.TrimSpace(k)
}

func addAttr(m map[string]string, t string) {
	if t == "" {
		return
	}
	if i := strings.IndexByte(t, '='); i >= 0 {
		m[t[:i]] = t[i+1:]
	} else {
		m[t] = ""
	}
}

func cloneFields(in map[string]string) map[string]string {
	out := make(map[string]string, len(in)+1)
	for k, v := range in {
		out[k] = v
	}
	return out
}
