# RECORDS golden samples (WS5 → WS3/WS4 handoff)

Real recorder-file samples collected from a running single-ASIC SONiC VS
(`vlab-01`, SONiC master, trixie) for parser/matcher development and golden
tests. These are the *actual* on-wire formats — write parsers against these, not
the idealized `ts|op|key|attr` sketch in the plan.

| File | Source | Contents |
|---|---|---|
| `swss.rec.sample` | live `/var/log/swss/swss.rec` (tail 200) | APPL_DB ops: `ROUTE_TABLE`, `NEIGH_TABLE`, ... `ts\|TABLE:key\|SET/DEL\|f:v...` |
| `sairedis.rec.sample` | live `/var/log/swss/sairedis.rec` (tail 200) | sairedis ops incl. bulk `\|\|` and api-name variants |
| `sairedis.rec.2.gz` | gzip of `/var/log/swss/sairedis.rec` (first 4000 lines) | real compressed content for gz-replay tests: `c/s/r` (create/set/remove), `C/S/R/B` bulk, `g/G` get+response, `q/Q` query+response, `n` notify |
| `retry.rec.sample` | live `/var/log/swss/retry.rec` | orchagent retry recorder (banners only on this box) |
| `sairedis_with_E.sample` | **hand-written** | synthetic failure fixture (see note) |

## Real sairedis format notes (important for WS3)

- Standard op: `ts|<op>|SAI_OBJECT_TYPE_X:<oid or {json}>|attr=val|...`
  - single ops are lowercase: `c` create, `r` remove, `s` set, `g` get, `p` counter-poll
- API-name variants (get/query): `ts|q|<api>|SAI_OBJECT_TYPE_X:...|...` and the
  response `ts|Q|<api>|SAI_STATUS_...|...`. The key token is the first field that
  starts with `SAI_OBJECT_TYPE_`; anything before it is the API name.
- Bulk ops are uppercase with `||` separators and the object type stated once:
  `ts|C|SAI_OBJECT_TYPE_ROUTE_ENTRY||{k1}|a1||{k2}|a2...` (same for `R`, `S`, `B`).
- Responses that carry a status: `G|<status>|...` (get), `Q|<api>|<status>|...`
  (query), `F|<status>` (flush fdb), `A|<status>`. Non-success values seen on this
  box: `SAI_STATUS_NOT_SUPPORTED`, `SAI_STATUS_NOT_IMPLEMENTED`,
  `SAI_STATUS_BUFFER_OVERFLOW` — all on `Q` capability probes, not programming ops.

## The `E` (failure) line — real, but only in sync mode on a box that can fail

A failed SAI create/set/remove **is** recorded as `E|<status>`, exactly as the
plan assumes — but only in **synchronous** sairedis mode (`redis_sync`/`zmq_sync`).
The path (see `sonic-sairedis`):

- `RedisRemoteSaiInterface::create/set/remove` writes the request line
  (`c|` / `s|` / `r|`), then calls `waitForResponse(...)`.
- `waitForResponse` (only when `m_syncMode`) waits for syncd's real status and
  calls `Recorder::recordGenericResponse(status)`.
- `recordGenericResponse` writes `E|<SAI_STATUS_...>` **only when status !=
  SAI_STATUS_SUCCESS** (`Recorder.cpp` ~L1315). Bulk ops use
  `recordBulkGenericResponse` → `E|<overall>|<per-object statuses>`.

Format: the `E` line carries **no key** — it belongs to the request line
immediately before it. So a parser attaches it to the last emitted sairedis
record (which is what `records_parser.go` does).

Why this fixture is hand-written: a virtual switch's SAI (`libsaivs`) returns
`SUCCESS` for every create/set/remove, so `status != SUCCESS` never triggers and
**no `E` line is ever produced on a VS**. That is a virtual-SAI limitation, not a
sairedis one — on real hardware in sync mode, `ops=E` lights up for genuine
programming failures. `sairedis_with_E.sample` lets us exercise the attach +
`ops=E` path on a VS where a real failure can't be produced.

Separately, get/query responses carry their own status via `G|<status>`,
`Q|<api>|<status>`, `F|<status>`, `A|<status>`. The non-success values seen on
this VS (`SAI_STATUS_NOT_SUPPORTED`, `NOT_IMPLEMENTED`, `BUFFER_OVERFLOW`) are all
capability probes on `Q` lines — distinct from the `E` create/set/remove failure
path.
