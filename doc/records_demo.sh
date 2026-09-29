#!/bin/bash
# records_demo.sh - reproducible demo of the RECORDS gNMI target.
#
# Assumes a telemetry server with the RECORDS target is listening (see
# doc/gnmi_records.md and the WS5 build/deploy notes). Point ADDR at it.
#
#   ADDR=127.0.0.1:8080 ROUTE=10.0.0.60/31 ./records_demo.sh
#
# The demo shows, against real swss.rec / sairedis.rec data:
#   1. replay of a route (from=)          - history off disk
#   2. exact key match + correlation      - swss SET + the SAI route entry
#   3. ops=DEL filter                     - deletions only
#   4. ops=E failures                     - needs a sync-mode box or an injected E
#   5. live tail                          - subscribe, then add/del a route
set -uo pipefail

ADDR=${ADDR:-127.0.0.1:8080}
ROUTE=${ROUTE:-10.0.0.60/31}
G="gnmic -a ${ADDR} --insecure sub --target RECORDS --mode stream"

hr() { printf '\n=== %s ===\n' "$1"; }

hr "1+2. Replay a route (from=-400d): swss SET + correlated SAI route entry"
timeout 8 $G --path "/RECORDS/localhost/APPL_DB/ROUTE_TABLE/${ROUTE}[from=-400d]" \
  | grep -oE '"(source|table|op|matched_by)": "[^"]*"' | paste - - - - | sort -u

hr "3. ops=DEL on the whole ROUTE_TABLE (deletions only)"
timeout 6 $G --path "/RECORDS/localhost/APPL_DB/ROUTE_TABLE[from=-400d][ops=DEL]" \
  | grep -oE '"op": "[^"]*"' | sort | uniq -c

hr "4. ops=E failures on the SAI route entry (inject a fixture on a VS first)"
timeout 6 $G --path "/RECORDS/localhost/ASIC_DB/ASIC_STATE/SAI_OBJECT_TYPE_ROUTE_ENTRY[from=-400d][ops=E]" \
  | grep -E '"status"|_response|matched_by' | head

hr "5. LIVE tail — run this, then in another shell: sudo config route add prefix 192.0.2.0/24 nexthop <nh>"
echo "timeout 30 $G --path \"/RECORDS/localhost/APPL_DB/ROUTE_TABLE\""
