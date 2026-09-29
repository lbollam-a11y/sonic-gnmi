# WS5 — Integration, build, demo (notes)

This captures what WS5 verified and the fast loop the whole team can reuse.

## Environment (single-ASIC SONiC VS)

Validated on a running SONiC master VS (`vlab-01`, Debian 13.6, glibc 2.41):

- Record files present and populated: `/var/log/swss/{swss.rec,sairedis.rec}` plus
  rotated `.1` and `.2.gz .. .5.gz`, and `retry.rec`.
- The gnmi container mounts host `/` read-only at `/mnt/host`, so the files are at
  `/mnt/host/var/log/swss/` — no new mount needed. `/etc/localtime` is mounted, so
  the container TZ matches the host (UTC on this box).
- Single namespace only (`localhost`); `asicN` stays unit-test-only per plan.

## Findings that shape the plan

1. **`synchronous_mode` is enabled, but there are zero `|E|` records** on the box,
   and attempts to force one (bogus nexthops, in-use deletes) produced none. The
   VS's virtual SAI never fails, so failure records cannot be produced naturally on
   a VS. `ops=E` is validated with a hand-written fixture
   (`testdata/records/sairedis_with_E.sample`) and injection.
2. **Real `sairedis.rec` is richer than `ts|op|key|attr`:** there are API-name
   variants (`q|attribute_capability|...`, `q|object_type_get_availability|...`) and
   bulk lines using `||` separators with the object type stated once
   (`S|SAI_OBJECT_TYPE_ROUTE_ENTRY||{k1}|a1||{k2}|a2...`). The parser keys off the
   `SAI_OBJECT_TYPE_` token to find the key regardless of variant.
3. **Toolchain/ABI skew:** the running trixie VS is fresh master (libhiredis.so.1,
   python3.13, **libyang.so.3**). Every local build tree vendors a CVL that uses the
   custom libyang **1.x** API (`ly_verb`, `lyd_free_withsiblings`,
   `lyd_node_union_matches_non_leafref`, ...) and cannot compile against libyang3 —
   that is the full upstream libyang1->3 CVL migration, not a patch. So a binary for
   the *trixie* gnmi container needs libyang3-compatible mgmt-common source (not on
   this host). **Resolution for the demo:** the real SONiC gnmi container image
   `docker-gnmi:latest` is Debian 12 (bookworm) with exactly the libs our binary
   links (libhiredis.so.0.14, libpython3.11, **libyang.so.1**). We run the RECORDS
   telemetry inside that real gnmi image (`recdemo-gnmi:latest` = docker-gnmi + redis)
   at the real `/usr/sbin/telemetry` path — no ABI shims. All bookworm sonic-vs
   images on the host are also bookworm, so the binary drops straight into their gnmi
   containers too.
4. **telemetry flag parsing:** `main()` parses telemetry flags via a private
   `flag.FlagSet` that rejects unknown flags, so `-records_dir` on the CLI is not
   accepted. RECORDS is therefore configured via env (`RECORDS_DIR`, `RECORDS_TZ`).

## Fast build loop (~35s warm)

Build the telemetry binary against a warm vendor tree inside a sonic-slave
container, then `docker cp` it into the gnmi container and restart:

```sh
# inside sonic-slave-<debian> with the repo + a warm vendor/ at /work/sonic-gnmi
GO=/work/go-dist/go/bin/go
export GOPATH=/work/gopath GOPROXY=off GOSUMDB=off GOTOOLCHAIN=local GOFLAGS=-buildvcs=false
export CGO_CFLAGS="-I/usr/include/swss" CGO_LDFLAGS="-lswsscommon -lhiredis"
cd /work/sonic-gnmi
$GO build -o build/bin/telemetry -mod=vendor -tags "gnmi_translib_write" \
   github.com/sonic-net/sonic-gnmi/telemetry

# deploy to the VS gnmi container
docker cp gnmi:/usr/sbin/telemetry /usr/sbin/telemetry.orig   # backup once
docker cp build/bin/telemetry gnmi:/usr/sbin/telemetry
docker exec gnmi supervisorctl restart gnmi-native
```

Match the sonic-slave Debian release to the target gnmi container (glibc /
libhiredis / libpython / libyang sonames must match).

## Demo (proven end-to-end on the VS's real data)

See `doc/records_demo.sh`. Verified **inside the real SONiC gnmi container image**
(`docker-gnmi:latest`, telemetry at `/usr/sbin/telemetry`): replay with `from=`,
exact key match, APPL_DB→sairedis correlation
(`matched_by:correlation:ROUTE_TABLE.dest`), `ops=DEL`/`ops=E` filters, and live
tail of a `config route add/del` on the switch.
