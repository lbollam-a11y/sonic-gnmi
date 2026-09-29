package client

// RECORDS matching + correlation (WS3 bootstrap).
//
// Match order (first hit wins):
//   1. Exact key match      (APPL_DB table+key, ASIC_DB type+oid/entry)
//   2. Table/type prefix    (empty key)
//   3. Correlation APPL->ASIC  (ROUTE_TABLE->ROUTE_ENTRY on dest, NEIGH->NEIGHBOR on ip)
//   4. Reverse correlation  ASIC->APPL (same table)
//   5. ops filter           (applied last)

import (
	"encoding/json"
	"net"
	"time"

	gnmipb "github.com/openconfig/gnmi/proto/gnmi"
)

// recSub is one parsed RECORDS subscription.
type recSub struct {
	namespace string
	db        string          // APPL_DB | ASIC_DB
	table     string          // ROUTE_TABLE ... | SAI_OBJECT_TYPE_ROUTE_ENTRY ...
	key       string          // "" means whole table
	from      time.Time       // zero = live only
	ops       map[string]bool // empty = all ops
	path      *gnmipb.Path    // echoed back on match
}

// correlation rule: APPL_DB table -> (sai object type, correlated value getter).
type corrRule struct {
	saiType string
	field   string // for matched_by text
	value   func(r *Record) string
}

var corrRules = map[string]corrRule{
	"ROUTE_TABLE": {
		saiType: "SAI_OBJECT_TYPE_ROUTE_ENTRY",
		field:   "dest",
		value:   func(r *Record) string { return jsonField(r.Key, "dest") },
	},
	"NEIGH_TABLE": {
		saiType: "SAI_OBJECT_TYPE_NEIGHBOR_ENTRY",
		field:   "ip",
		value:   func(r *Record) string { return jsonField(r.Key, "ip") },
	},
	"FDB_TABLE": {
		saiType: "SAI_OBJECT_TYPE_FDB_ENTRY",
		field:   "mac",
		value:   func(r *Record) string { return jsonField(r.Key, "mac") },
	},
}

type recordsMatcher struct {
	subs []*recSub
}

func newRecordsMatcher(subs []*recSub) *recordsMatcher {
	return &recordsMatcher{subs: subs}
}

// Match satisfies the Matcher interface.
func (m *recordsMatcher) Match(r *Record) (bool, string) {
	_, how, ok := m.MatchSub(r)
	return ok, how
}

// MatchSub returns the matched subscription (for path echo), the matched_by
// string, and whether any subscription wanted the record.
func (m *recordsMatcher) MatchSub(r *Record) (*recSub, string, bool) {
	for _, s := range m.subs {
		if how, ok := matchOne(s, r); ok {
			if !opAllowed(s, r) {
				continue
			}
			return s, how, true
		}
	}
	return nil, "", false
}

func matchOne(s *recSub, r *Record) (string, bool) {
	// 1 & 2: same DB and table -> exact key or prefix.
	if s.db == r.DB && s.table == r.Table {
		if s.key == "" {
			return "prefix:" + r.Table, true
		}
		if keyEqual(s.key, r.Key) || jsonKeyEqual(s.key, r.Key) {
			return "key", true
		}
	}

	// 3: forward correlation APPL_DB subscription -> sairedis record.
	if s.db == dbAPPL && r.Source == srcSairedis {
		if rule, ok := corrRules[s.table]; ok && r.Table == rule.saiType {
			v := rule.value(r)
			if v != "" && (s.key == "" || keyEqual(v, applKeyValue(s.table, s.key))) {
				return "correlation:" + s.table + "." + rule.field, true
			}
		}
	}

	// 4: reverse correlation ASIC_DB subscription -> swss record.
	if s.db == dbASIC && r.Source == srcSwss {
		for applTable, rule := range corrRules {
			if s.table == rule.saiType && r.Table == applTable {
				if s.key == "" {
					return "reverse:" + applTable, true
				}
				// s.key is a sai type[:entry]; correlate on the extracted value.
				if v := jsonField(s.key, rule.field); v != "" && keyEqual(v, applKeyValue(applTable, r.Key)) {
					return "reverse:" + applTable + "." + rule.field, true
				}
			}
		}
	}

	return "", false
}

func opAllowed(s *recSub, r *Record) bool {
	if len(s.ops) == 0 {
		return true
	}
	// E means "failures only": match when the record carries a failure status.
	if s.ops["E"] {
		if r.Status != "" && r.Status != saiStatusOK && r.Fields["_response"] == "E" {
			return true
		}
		if len(s.ops) == 1 {
			return false
		}
	}
	return s.ops[r.Op]
}

// applKeyValue extracts the correlatable value from an APPL_DB key. For
// ROUTE_TABLE the key IS the prefix; for NEIGH_TABLE it is "<intf>:<ip>".
func applKeyValue(table, key string) string {
	switch table {
	case "NEIGH_TABLE":
		// first ':' separates interface from ip (ip may contain ':').
		for i := 0; i < len(key); i++ {
			if key[i] == ':' {
				return key[i+1:]
			}
		}
		return key
	default:
		return key
	}
}

// jsonField pulls a string field out of a sairedis JSON entry key.
func jsonField(jsonKey, field string) string {
	if len(jsonKey) == 0 || jsonKey[0] != '{' {
		return ""
	}
	var m map[string]string
	if err := json.Unmarshal([]byte(jsonKey), &m); err != nil {
		return ""
	}
	return m[field]
}

// keyEqual compares two keys, tolerating CIDR / bare-IP spelling differences.
func keyEqual(a, b string) bool {
	if a == b {
		return true
	}
	if _, na, ea := net.ParseCIDR(a); ea == nil {
		if _, nb, eb := net.ParseCIDR(b); eb == nil {
			return na.String() == nb.String()
		}
	}
	if ia, ib := net.ParseIP(a), net.ParseIP(b); ia != nil && ib != nil {
		return ia.Equal(ib)
	}
	return false
}

// jsonKeyEqual lets an operator type a route prefix and match a sairedis JSON
// entry key whose "dest" equals it.
func jsonKeyEqual(subKey, recKey string) bool {
	if len(recKey) > 0 && recKey[0] == '{' {
		if d := jsonField(recKey, "dest"); d != "" && keyEqual(subKey, d) {
			return true
		}
		if ip := jsonField(recKey, "ip"); ip != "" && keyEqual(subKey, ip) {
			return true
		}
	}
	return false
}
