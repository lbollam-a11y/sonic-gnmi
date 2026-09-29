# RECORDS gNMI target — follow the route through the recorder files

`RECORDS` is a gNMI **STREAM**-only subscription target that streams the orchagent
and sairedis recorder files (`/var/log/swss/swss.rec`, `sairedis.rec`) over gNMI.
Name a key or a table in APPL_DB or ASIC_DB and get one update per matching
record — history first if you ask for it with `from=`, then a live tail. It does
not touch orchagent or sairedis; it is a read-only cursor over the files already
on disk (mounted read-only into the gnmi container at `/mnt/host/var/log/swss`).

## Path grammar

```
/RECORDS/<namespace>/<DB>/<TABLE>[/<key elements...>][from=<when>][ops=<list>]

namespace  localhost on a single-ASIC box; asic0, asic1, ... on multi-ASIC.
DB         APPL_DB | ASIC_DB
TABLE      APPL_DB: table as in swss.rec (ROUTE_TABLE, NEIGH_TABLE, FDB_TABLE, ...)
           ASIC_DB: always ASIC_STATE, followed by SAI_OBJECT_TYPE_<X>[:<oid|entry>]
key        APPL_DB: remaining path elements re-joined with "/" (route prefixes are
           split by gNMI and stitched back). Empty key = whole table.
from       RFC3339, epoch seconds, or relative (-30m, -2h, -1d). Absent = live only.
ops        APPL_DB: SET, DEL.  ASIC_DB: sairedis opcodes c r s g C R S G B E ...
           E = failures only. Comma-separated. Absent = everything.
```

### Examples

```
/RECORDS/localhost/APPL_DB/ROUTE_TABLE/10.1.0.0/24[from=-30m]
/RECORDS/localhost/APPL_DB/NEIGH_TABLE
/RECORDS/localhost/ASIC_DB/ASIC_STATE/SAI_OBJECT_TYPE_ROUTE_ENTRY[ops=E]
/RECORDS/asic1/ASIC_DB/ASIC_STATE/SAI_OBJECT_TYPE_NEXT_HOP_GROUP[from=2026-09-26T10:00:00]
```

## Output

Each record is one gNMI Notification with one JSON_IETF Update. The Notification
timestamp is the record's own timestamp from the file, not the wall clock:

```json
{
  "seq": "sairedis:1180422:9932711",
  "ts": "2026-09-26.10:15:32.123456",
  "source": "sairedis",
  "db": "ASIC_DB",
  "table": "SAI_OBJECT_TYPE_ROUTE_ENTRY",
  "key": "{\"dest\":\"10.1.0.0/24\",...}",
  "op": "c",
  "fields": {"SAI_ROUTE_ENTRY_ATTR_NEXT_HOP_ID": "oid:0x5000000000a3c"},
  "status": "",
  "matched_by": "correlation:ROUTE_TABLE.dest"
}
```

Two synthetic markers help the operator tell history from tail:
`{"event":"replay_start"}` before replay, `{"event":"live"}` when it finishes.

`matched_by` explains why a record was delivered: `key`, `prefix:<table>`,
`correlation:<table>.<field>`, or `reverse:<table>`.

## Configuration (WS5 container wiring)

The recorder directory and timezone are read from the environment (telemetry's
`main()` parses its flags through a private `flag.FlagSet`, so a package-level
flag is not accepted on the telemetry command line):

```sh
export RECORDS_DIR=/mnt/host/var/log/swss   # default when unset
export RECORDS_TZ=UTC                        # empty = container local time
```

The gnmi container start script (`gnmi-native.sh` / `telemetry.sh`) exports these
before launching telemetry.

## Limits (state them plainly)

- **`E` (failure) records only exist when sairedis runs in a sync mode**
  (`redis_sync` / `zmq_sync`). In async mode failures never reach the file. A
  **virtual switch never produces `E` records** — its virtual SAI accepts almost
  everything — so `ops=E` is validated on the VS with a hand-written fixture
  (`testdata/records/sairedis_with_E.sample`).
- Timestamps in the files are **localtime with no zone**; parsed in `RECORDS_TZ`
  (or container local time).
- STREAM only. POLL/ONCE/Get/Set return Unimplemented.
