#!/usr/bin/env python3
# =========================================================================
# transmission-pdf-audit.py
#
# Exhaustive, resumable candidate audit of submission transmissions (type 7)
# in Dialogporten whose generated PDF receipt (data type "ref-data-as-pdf")
# may be missing, plus the per-instance export a backfill needs.
#
# This is a CANDIDATE audit over Dialogporten data. It cannot see Storage, so
# it cannot prove that a receipt was generated for a given instance. It is
# exhaustive for transmissions WITHOUT any receipt marker, subject to the
# rolling-observation limitation described under "run".
#
# Subcommands
#   run        ENV FROM TO OUTDIR   walk the range in chunks (resumable)
#   fetch-meta OUTDIR               app metadata from Resource Registry + apps
#   report     OUTDIR               integrity checks + counts (sqlite)
#   export     OUTDIR               backfill candidate list with Storage ids
#   explain    ENV FROM TO          EXPLAIN (ANALYZE, BUFFERS) for one chunk
#
# Chunking
#   DialogTransmission.Id is UUIDv7, so an Id range is a time range served by
#   the primary key. The range [FROM, TO) is cut into logical one-hour
#   intervals identified by their UTC millisecond bounds (the last interval is
#   clipped to TO). Each interval is queried as one statement under a 15 s
#   statement_timeout; a timeout splits the interval in half down to a
#   5 minute minimum, below which the leaf is recorded as failed.
#
# Recovery model
#   Every successful leaf is one gzip'ed JSON file holding BOTH its rows and
#   its coverage record, written to a temp name and atomically renamed into
#   OUTDIR/chunks/gen<N>/. There is never a result without coverage or vice
#   versa. A manifest freezes env, bounds and query version; a resume against
#   a different manifest is refused. A lock file rejects concurrent writers.
#
# Generations
#   A re-scan (--rescan-days) writes a new generation. The report selects, per
#   logical interval, the newest generation whose successful leaves tile the
#   interval completely; otherwise the previous generation is kept for the
#   whole interval. Generations are never mixed inside one interval.
#
# Classes (exactly one per transmission, precedence top to bottom)
#   empty        zero attachments
#   marker       an attachment named ref-data-as-pdf exists
#   no_marker    all attachments have a Name and none is the marker
#   pdf_unknown  some attachment has no Name, and some attachment has an
#                application/pdf URL: receipt identity undecidable in DP
#   no_pdf_url   some attachment has no Name, and no application/pdf URL
#
# Requires: python3 (3.9+), psql. For staging/prod: a tunnel from
# ../db-access/forward.sh and an active PIM activation. Runs as the Entra
# readonly group and is fully audited. Nothing is written to the database.
# =========================================================================
import argparse
import csv
import datetime as dt
import fcntl
import glob
import gzip
import hashlib
import json
import os
import re
import sqlite3
import subprocess
import sys
import tempfile
import time
import zoneinfo
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, List, Optional, Tuple

SCRIPT_VERSION = "1"
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
DB_ACCESS_DIR = os.path.join(SCRIPT_DIR, "..", "db-access")
STMT_TIMEOUT = os.environ.get("STMT_TIMEOUT", "15s")
HOUR_MS = 3600 * 1000
MIN_CHUNK_MS = 5 * 60 * 1000
TOKEN_TTL_S = 40 * 60
TZ_NAME = os.environ.get("TZ_NAME", "Europe/Oslo")
TZ = zoneinfo.ZoneInfo(TZ_NAME)
CLASSES = ("empty", "marker", "no_marker", "pdf_unknown", "no_pdf_url")
CANDIDATE_CLASSES = tuple(c for c in CLASSES if c != "marker")
# Adapter PR #224 reached production on 2026-06-15 (Release-184, main@7a2a1d0a).
PERIOD_BOUNDARY = dt.datetime(2026, 6, 15, tzinfo=TZ)
STORAGE_LABEL_RE = re.compile(
    r"^urn:altinn:integration:storage:(?P<party>\d+)/(?P<guid>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})$",
    re.IGNORECASE,
)
RESOURCE_REGISTRY = "https://platform.altinn.no/resourceregistry/api/v1/resource/resourcelist?includeApps=true&includeAltinn2=false&includeMigratedApps=false"

# One statement per chunk. One membership row per type-7 transmission in the
# Id range. The URL probe only runs for rows that reach the pdf_unknown branch
# (CASE evaluates branches lazily). All aggregates are COALESCEd so an empty
# or all-null input cannot vanish through SQL null semantics.
CHUNK_SQL = """/*+ Leading((t d)) NestLoop(t d) IndexScan(t PK_DialogTransmission) IndexOnlyScan(d IX_Dialog_Id_Covering_V2) */
SELECT t."Id", t."DialogId", d."Org", d."ServiceResource", d."Deleted",
       (SELECT min(s."CreatedAt") FROM "Actor" s WHERE s."TransmissionId" = t."Id"),
       coalesce(a.n, 0), coalesce(a.cls, 'empty')
FROM "DialogTransmission" t
JOIN "Dialog" d ON d."Id" = t."DialogId"
LEFT JOIN LATERAL (
  SELECT count(*) AS n,
         CASE
           WHEN count(*) = 0 THEN 'empty'
           WHEN coalesce(bool_or(x."Name" = 'ref-data-as-pdf'), false) THEN 'marker'
           WHEN coalesce(bool_and(x."Name" IS NOT NULL), false) THEN 'no_marker'
           WHEN EXISTS (SELECT 1 FROM "Attachment" x2 JOIN "AttachmentUrl" u ON u."AttachmentId" = x2."Id"
                        WHERE x2."TransmissionId" = t."Id" AND u."MediaType" ILIKE 'application/pdf%') THEN 'pdf_unknown'
           ELSE 'no_pdf_url'
         END AS cls
  FROM "Attachment" x WHERE x."TransmissionId" = t."Id"
) a ON true
WHERE t."TypeId" = 7 AND t."Id" >= '{lo}'::uuid AND t."Id" < '{hi}'::uuid
"""
QUERY_VERSION = hashlib.sha256(CHUNK_SQL.encode()).hexdigest()[:12]

LABEL_SQL = """SELECT "DialogServiceOwnerContextId", "Value"
FROM "DialogServiceOwnerLabel"
WHERE "DialogServiceOwnerContextId" = ANY('{{{ids}}}'::uuid[])
  AND "Value" LIKE 'urn:altinn:integration:storage:%'
"""


# ------------------------------------------------------------------ utils ---
def die(msg: str, code: int = 2) -> None:
    print(msg, file=sys.stderr)
    sys.exit(code)


def uuid7_lower(ms: int) -> str:
    h = "%012x" % ms
    return "%s-%s-7000-8000-000000000000" % (h[:8], h[8:12])


def uuid_ms(u: str) -> int:
    return int(u.replace("-", "")[:12], 16)


def parse_local(s: str) -> int:
    """ISO date/datetime in TZ_NAME -> UTC ms. 'now' -> current time."""
    if s == "now":
        return int(time.time() * 1000)
    d = dt.datetime.fromisoformat(s)
    if d.tzinfo is None:
        d = d.replace(tzinfo=TZ)
    return int(d.timestamp() * 1000)


def fmt_ms(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, TZ).strftime("%Y-%m-%d %H:%M")


def utc_iso(ms: Optional[int] = None) -> str:
    t = dt.datetime.now(dt.timezone.utc) if ms is None else dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc)
    return t.isoformat(timespec="seconds")


def atomic_write(path: str, data: bytes) -> None:
    os.makedirs(os.path.dirname(path), exist_ok=True)
    fd, tmp = tempfile.mkstemp(dir=os.path.dirname(path), prefix=".tmp-")
    try:
        with os.fdopen(fd, "wb") as f:
            f.write(data)
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, path)
    finally:
        if os.path.exists(tmp):
            os.unlink(tmp)


def month_label(ms: int, tz: zoneinfo.ZoneInfo) -> str:
    """Month of a UTC instant in the run's zone, using that instant's own offset,
    so the label does not depend on the offset in force when the report runs."""
    return dt.datetime.fromtimestamp(ms / 1000, tz).strftime("%Y-%m")


def logical_intervals(from_ms: int, to_ms: int) -> List[Tuple[int, int]]:
    out = []
    cur = from_ms
    while cur < to_ms:
        nxt = min(cur + HOUR_MS, to_ms)
        out.append((cur, nxt))
        cur = nxt
    return out


# --------------------------------------------------------------- database ---
class Db:
    """psql wrapper. ENV -> tunnel port + Entra group + token; --dsn overrides."""

    PORTS = {"test": 25432, "yt01": 35432, "staging": 45432, "prod": 55432}

    def __init__(self, env: Optional[str], dsn: Optional[str]):
        self.env = env
        self.dsn = dsn
        self._token = None
        self._token_at = 0.0
        if dsn is None:
            if env not in self.PORTS:
                die("ENV must be one of %s, or pass --dsn" % ", ".join(self.PORTS))
            group = "altinn-dialogporten-test-postgresql-readonly" if env in ("test", "yt01") else "altinn-dialogporten-prod-postgresql-readonly"
            self.dsn = "host=localhost port=%d dbname=dialogporten user=%s sslmode=require" % (self.PORTS[env], group)
            rc = subprocess.run(["nc", "-z", "localhost", str(self.PORTS[env])], capture_output=True).returncode
            if rc != 0:
                die("No tunnel on localhost:%d. Start it: %s/forward.sh -e %s -t postgres" % (self.PORTS[env], DB_ACCESS_DIR, env), 1)

    def _password(self) -> Optional[str]:
        if self.env is None:
            return os.environ.get("PGPASSWORD")
        if self._token is None or time.time() - self._token_at > TOKEN_TTL_S:
            r = subprocess.run([os.path.join(DB_ACCESS_DIR, "pg-token.sh"), self.env], capture_output=True, text=True)
            if r.returncode != 0:
                die("pg-token.sh failed: %s" % r.stderr.strip(), 1)
            self._token = r.stdout.strip()
            self._token_at = time.time()
        return self._token

    def query(self, sql: str, timeout: str = STMT_TIMEOUT) -> Tuple[int, List[List[str]], str]:
        """Returns (rc, rows, stderr). rc 0 ok, 3 statement timeout, 4 auth, 1 other."""
        env = dict(os.environ)
        pw = self._password()
        if pw:
            env["PGPASSWORD"] = pw
        env["PGAPPNAME"] = "transmission-pdf-audit"
        r = subprocess.run(
            ["psql", self.dsn, "-X", "-q", "-At", "-F", "\t", "-v", "ON_ERROR_STOP=1",
             "-c", "SET statement_timeout = '%s'" % timeout, "-c", sql],
            capture_output=True, text=True, env=env,
        )
        if r.returncode != 0:
            err = r.stderr
            if "statement timeout" in err:
                return 3, [], err
            if "password authentication failed" in err or "isn't a member" in err or "Entra" in err:
                return 4, [], err
            return 1, [], err
        rows = [line.split("\t") for line in r.stdout.splitlines() if line]
        return 0, rows, ""

    def identity(self) -> str:
        """Stable identity of the database behind this connection, so a resume
        cannot silently mix databases. Falls back to the DSN without password."""
        rc, rows, _ = self.query("SELECT current_database() || '/' || (SELECT system_identifier FROM pg_control_system())")
        if rc == 0 and rows and rows[0] and rows[0][0]:
            return "db:" + rows[0][0]
        redacted = " ".join(p for p in (self.dsn or "").split() if not p.lower().startswith("password="))
        return "dsn:" + hashlib.sha256(redacted.encode()).hexdigest()[:16]


# ----------------------------------------------------------------- chunks ---
def chunk_dir(out: str, gen: int) -> str:
    return os.path.join(out, "chunks", "gen%d" % gen)


def leaf_path(out: str, gen: int, lo: int, hi: int) -> str:
    return os.path.join(chunk_dir(out, gen), "%d-%d.json.gz" % (lo, hi))


def failed_path(out: str, gen: int, lo: int, hi: int) -> str:
    return os.path.join(chunk_dir(out, gen), "%d-%d.failed.json" % (lo, hi))


def list_leaves(out: str, gen: int) -> Tuple[List[Tuple[int, int]], List[Tuple[int, int]]]:
    """(ok leaves, failed leaves) as (lo, hi) lists for one generation."""
    ok, failed = [], []
    for p in glob.glob(os.path.join(chunk_dir(out, gen), "*.json.gz")):
        m = re.match(r"(\d+)-(\d+)\.json\.gz$", os.path.basename(p))
        if m:
            ok.append((int(m.group(1)), int(m.group(2))))
    for p in glob.glob(os.path.join(chunk_dir(out, gen), "*.failed.json")):
        m = re.match(r"(\d+)-(\d+)\.failed\.json$", os.path.basename(p))
        if m:
            failed.append((int(m.group(1)), int(m.group(2))))
    return sorted(ok), sorted(failed)


def list_gens(out: str) -> List[int]:
    gens = []
    for p in glob.glob(os.path.join(out, "chunks", "gen*")):
        m = re.match(r"gen(\d+)$", os.path.basename(p))
        if m:
            gens.append(int(m.group(1)))
    return sorted(gens)


def tiles(leaves: List[Tuple[int, int]], lo: int, hi: int) -> bool:
    """Do the leaves inside [lo,hi) tile it exactly, no gaps, no overlaps?"""
    inside = sorted(l for l in leaves if l[0] >= lo and l[1] <= hi)
    cur = lo
    for a, b in inside:
        if a != cur:
            return False
        cur = b
    return cur == hi


def covered_ms(leaves: List[Tuple[int, int]], lo: int, hi: int) -> int:
    return sum(min(b, hi) - max(a, lo) for a, b in leaves if a < hi and b > lo)


# ------------------------------------------------------------------- run ----
class Runner:
    def __init__(self, db: Db, out: str, gen: int, base_pause: float, max_chunk_ms: int):
        self.db = db
        self.out = out
        self.gen = gen
        self.base_pause = base_pause
        self.pause = base_pause
        self.max_chunk_ms = max_chunk_ms
        self.ok_leaves, self.failed_leaves = list_leaves(out, gen)

    def _throttle(self, seconds: float) -> None:
        # Back off while the database is slow; decay back to the base pause.
        if seconds > 8:
            self.pause = min(self.pause * 2, 10.0)
        else:
            self.pause = max(self.base_pause, self.pause / 2)
        time.sleep(self.pause)

    def run_leaf(self, lo: int, hi: int) -> int:
        t0 = time.time()
        rc, rows, err = self.db.query(CHUNK_SQL.format(lo=uuid7_lower(lo), hi=uuid7_lower(hi)))
        seconds = time.time() - t0
        if rc == 4:
            die("Database authentication failed (PIM activation expired?):\n%s" % err.strip(), 1)
        if rc == 1:
            die("Hard error from the database, aborting:\n%s" % err.strip(), 1)
        if rc == 3:
            print("  split  %s -> %s  (timeout after %.1fs)" % (fmt_ms(lo), fmt_ms(hi), seconds))
            self._throttle(seconds)
            return 3
        for r in rows:
            if len(r) != 8:
                die("Unexpected row shape from psql: %r" % r, 1)
            if r[7] not in CLASSES:
                die("Unexpected class %r in row %r" % (r[7], r), 1)
            ms = uuid_ms(r[0])
            if not (lo <= ms < hi):
                die("Row %s has time %d outside chunk [%d,%d)" % (r[0], ms, lo, hi), 1)
        payload = {
            "lo": lo, "hi": hi, "gen": self.gen, "observed_at": utc_iso(), "seconds": round(seconds, 2),
            "query_version": QUERY_VERSION, "rows": rows,
        }
        atomic_write(leaf_path(self.out, self.gen, lo, hi), gzip.compress(json.dumps(payload).encode()))
        self.ok_leaves.append((lo, hi))
        print("  ok     %s -> %s  (%.1fs, %d rows)" % (fmt_ms(lo), fmt_ms(hi), seconds, len(rows)))
        self._throttle(seconds)
        return 0

    def walk(self, lo: int, hi: int) -> None:
        if (lo, hi) in self.ok_leaves:
            return
        if (lo, hi) in self.failed_leaves:
            # A leaf that timed out at minimum size on an earlier run: retry it, and
            # clear the failure record on success. It is never silently skipped.
            print("  retry  %s -> %s  (failed on an earlier run)" % (fmt_ms(lo), fmt_ms(hi)))
            if self.run_leaf(lo, hi) == 0:
                os.unlink(failed_path(self.out, self.gen, lo, hi))
                self.failed_leaves.remove((lo, hi))
            else:
                print("  FAIL   %s -> %s  still timing out at minimum chunk size" % (fmt_ms(lo), fmt_ms(hi)))
            return
        if hi - lo > self.max_chunk_ms or any(a < hi and b > lo for a, b in self.ok_leaves + self.failed_leaves):
            # Too large for the configured leaf size, or a previous run already
            # split this interval: never re-run the parent (double counting).
            mid = lo + (hi - lo) // 2
            self.walk(lo, mid)
            self.walk(mid, hi)
            return
        rc = self.run_leaf(lo, hi)
        if rc == 3:
            if hi - lo <= MIN_CHUNK_MS:
                atomic_write(failed_path(self.out, self.gen, lo, hi),
                             json.dumps({"lo": lo, "hi": hi, "gen": self.gen, "at": utc_iso()}).encode())
                self.failed_leaves.append((lo, hi))
                print("  FAIL   %s -> %s  timed out at minimum chunk size" % (fmt_ms(lo), fmt_ms(hi)))
                return
            mid = lo + (hi - lo) // 2
            self.walk(lo, mid)
            self.walk(mid, hi)


def load_manifest(out: str) -> Optional[dict]:
    p = os.path.join(out, "manifest.json")
    if not os.path.exists(p):
        return None
    with open(p) as f:
        return json.load(f)


def acquire_lock(out: str) -> int:
    """Exclusive OS-level lock on OUTDIR/lock, held for the whole run. Taken
    BEFORE the manifest is read or created, so two runs cannot both pass a
    check and then race on the manifest or the chunk files."""
    os.makedirs(out, exist_ok=True)
    fd = os.open(os.path.join(out, "lock"), os.O_RDWR | os.O_CREAT, 0o644)
    try:
        fcntl.flock(fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
    except OSError:
        os.close(fd)
        die("Another run holds %s/lock. Refusing to write concurrently." % out, 1)
    os.ftruncate(fd, 0)
    os.write(fd, str(os.getpid()).encode())
    return fd


def release_lock(fd: int) -> None:
    fcntl.flock(fd, fcntl.LOCK_UN)
    os.close(fd)


def cmd_run(a: argparse.Namespace) -> None:
    out = a.outdir
    lock = acquire_lock(out)
    try:
        db = Db(a.env, a.dsn)
        db_identity = db.identity()
        manifest = load_manifest(out)
        from_ms = parse_local(a.frm)
        if from_ms % HOUR_MS != 0:
            die("FROM must be on a whole hour (UTC), got %s" % a.frm)
        to_ms = parse_local(a.to)
        if manifest is None:
            manifest = {
                "schema": 2, "script_version": SCRIPT_VERSION, "env": a.env or "dsn", "dsn": a.dsn, "db_identity": db_identity,
                "from_ms": from_ms, "to_ms": to_ms, "from": fmt_ms(from_ms), "to": fmt_ms(to_ms),
                "tz": TZ_NAME, "query_version": QUERY_VERSION, "created_at": utc_iso(),
                "period_boundary_ms": int(PERIOD_BOUNDARY.timestamp() * 1000), "meta_sha": None,
            }
            atomic_write(os.path.join(out, "manifest.json"), json.dumps(manifest, indent=1).encode())
            print("New run against %s. TO frozen at %s (%s)." % (db_identity, fmt_ms(to_ms), TZ_NAME))
        else:
            mism = [k for k, v in (("env", a.env or "dsn"), ("query_version", QUERY_VERSION), ("db_identity", db_identity), ("tz", TZ_NAME))
                    if manifest.get(k) != v]
            if manifest["from_ms"] != from_ms:
                mism.append("from")
            if a.to != "now" and manifest["to_ms"] != to_ms:
                mism.append("to")
            if mism:
                die("Resume refused: %s differ from manifest in %s. Use another OUTDIR." % (", ".join(mism), out))
            to_ms = manifest["to_ms"]
            print("Resuming run created %s against %s, TO frozen at %s." % (manifest["created_at"], db_identity, fmt_ms(to_ms)))

        gen = a.gen
        if a.max_chunk_ms > HOUR_MS:
            die("--max-chunk-ms must not exceed one hour: leaves must lie within one logical interval")
        intervals = logical_intervals(from_ms, to_ms)
        if a.rescan_days:
            # Re-scan only the last N days before TO, as a new generation.
            if gen == 1:
                gen = (list_gens(out) or [1])[-1] + 1
            cutoff = to_ms - a.rescan_days * 24 * HOUR_MS
            intervals = [iv for iv in intervals if iv[1] > cutoff]
            print("Re-scan of the last %d days as generation %d (%d logical intervals)." % (a.rescan_days, gen, len(intervals)))
        runner = Runner(db, out, gen, a.pause, a.max_chunk_ms)
        print("Walking %d logical intervals %s .. %s on %s, gen %d, statement_timeout=%s, query %s"
              % (len(intervals), fmt_ms(from_ms), fmt_ms(to_ms), manifest["env"], gen, STMT_TIMEOUT, QUERY_VERSION))
        done = 0
        for lo, hi in intervals:
            if tiles(runner.ok_leaves, lo, hi):
                done += 1
                continue
            runner.walk(lo, hi)
            done += 1
            if done % 24 == 0:
                print("progress: %d/%d intervals, up to %s" % (done, len(intervals), fmt_ms(hi)))
        print("Done. Next: %s report %s" % (sys.argv[0], out))
    finally:
        release_lock(lock)


def cmd_explain(a: argparse.Namespace) -> None:
    db = Db(a.env, a.dsn)
    lo = parse_local(a.frm)
    hi = parse_local(a.to)
    sql = "EXPLAIN (ANALYZE, BUFFERS) " + CHUNK_SQL.format(lo=uuid7_lower(lo), hi=uuid7_lower(hi))
    rc, rows, err = db.query(sql, timeout=a.timeout)
    if rc != 0:
        die("EXPLAIN failed (rc %d):\n%s" % (rc, err.strip()), 1)
    for r in rows:
        print("\t".join(r))


# ------------------------------------------------------------- fetch-meta ---
def _fetch_json(url: str, timeout: int = 20) -> Tuple[int, Optional[object]]:
    import urllib.request
    import urllib.error
    try:
        with urllib.request.urlopen(url, timeout=timeout) as r:
            return r.status, json.loads(r.read().decode())
    except urllib.error.HTTPError as e:
        return e.code, None
    except Exception:
        return 0, None


def _classify_app(d: dict) -> dict:
    dts = d.get("dataTypes") or []
    pdf_types = [t for t in dts if t.get("appLogic") and t.get("enablePdfCreation")]
    risk = []
    for task in sorted({t.get("taskId") for t in pdf_types if t.get("taskId")}):
        first = next((t for t in dts if t.get("taskId") == task), None)
        if first and not first.get("appLogic") and (first.get("minCount") or 0) == 0:
            risk.append("%s:%s" % (task, first.get("id")))
    sas = (d.get("messageBoxConfig") or {}).get("syncAdapterSettings") or {}
    return {
        "pdf_expected": bool(pdf_types), "n_pdf_types": len(pdf_types),
        "d2_exposed": bool(risk), "first_optional": ";".join(risk),
        "nuget": d.get("altinnNugetVersion") or "", "disable_tx": bool(sas.get("disableAddTransmissions")),
    }


def cmd_fetch_meta(a: argparse.Namespace) -> None:
    out = a.outdir
    os.makedirs(out, exist_ok=True)
    status, lst = _fetch_json(RESOURCE_REGISTRY, timeout=60)
    if status != 200 or not isinstance(lst, list):
        die("Resource Registry resourcelist failed (http %s)" % status, 1)
    apps = [(r["identifier"], (r.get("hasCompetentAuthority") or {}).get("orgcode", "")) for r in lst if r.get("resourceType") == "AltinnApp"]
    print("%d AltinnApp resources" % len(apps))

    def one(item):
        rid, org = item
        rest = rid[len("app_"):]
        o, app = rest.split("_", 1)
        st, d = _fetch_json("https://%s.apps.altinn.no/%s/%s/api/v1/applicationmetadata" % (o, o, app))
        row = {"resource": rid, "org": org, "status": "ok" if st == 200 and isinstance(d, dict) and "dataTypes" in d else "http_%s" % st}
        if row["status"] == "ok":
            row.update(_classify_app(d))
        return row

    fields = ["resource", "org", "status", "pdf_expected", "n_pdf_types", "d2_exposed", "first_optional", "nuget", "disable_tx"]
    if os.path.exists(os.path.join(out, "meta.csv")) and not a.force:
        die("%s/meta.csv exists. A run binds to the metadata snapshot it first used; pass --force to replace it (report/export will then need --rebind-meta)." % out)
    with ThreadPoolExecutor(max_workers=8) as ex:
        rows = list(ex.map(one, apps))
    rows.sort(key=lambda r: r["resource"])
    body = []
    for r in rows:
        body.append([str(r.get(f, "")) for f in fields])
    data = ",".join(fields) + "\n" + "\n".join(",".join(x) for x in body) + "\n"
    atomic_write(os.path.join(out, "meta.csv"), data.encode())
    sha = hashlib.sha256(data.encode()).hexdigest()[:12]
    summary = {"fetched_at": utc_iso(), "apps": len(rows), "ok": sum(r["status"] == "ok" for r in rows),
               "pdf_expected": sum(1 for r in rows if r.get("pdf_expected")), "sha": sha}
    atomic_write(os.path.join(out, "meta.json"), json.dumps(summary, indent=1).encode())
    print("meta.csv written: %s" % json.dumps(summary))


def load_meta(out: str, manifest: dict, rebind: bool = False) -> Dict[str, dict]:
    """Load meta.csv and bind its hash to the run manifest on first use. A later
    load with different metadata is refused unless --rebind-meta is given, so a
    report or export cannot silently change with whatever was fetched last."""
    p = os.path.join(out, "meta.csv")
    bound = manifest.get("meta_sha")
    if not os.path.exists(p):
        if bound is not None and not rebind:
            die("meta.csv is missing but snapshot %s is bound to this run. Restore it, or pass --rebind-meta to continue deliberately without metadata." % bound, 3)
        if bound is not None and rebind:
            manifest["meta_sha"] = None
            atomic_write(os.path.join(out, "manifest.json"), json.dumps(manifest, indent=1).encode())
            print("Metadata binding %s removed from this run; app grouping unavailable." % bound)
        return {}
    with open(p, "rb") as f:
        data = f.read()
    sha = hashlib.sha256(data).hexdigest()[:12]
    if bound is None or rebind:
        manifest["meta_sha"] = sha
        atomic_write(os.path.join(out, "manifest.json"), json.dumps(manifest, indent=1).encode())
        print("Metadata snapshot %s %s to this run." % (sha, "rebound" if bound else "bound"))
    elif bound != sha:
        die("meta.csv (sha %s) differs from the snapshot bound to this run (%s). Restore it or pass --rebind-meta deliberately." % (sha, bound), 3)
    return {r["resource"]: r for r in csv.DictReader(data.decode().splitlines())}


def app_group(resource: str, meta: Dict[str, dict]) -> str:
    rid = resource.replace("urn:altinn:resource:", "")
    if not rid.startswith("app_"):
        return "non_app"
    if "_a2-" in rid:
        return "a2"
    m = meta.get(rid)
    if not m or m.get("status") != "ok":
        return "unknown"
    return "a3_pdf" if m.get("pdf_expected") == "True" else "a3_nopdf"


# ----------------------------------------------------------------- report ---
class Loaded:
    def __init__(self):
        self.con = sqlite3.connect(":memory:")
        self.invalid: List[str] = []
        self.gaps: List[Tuple[int, int]] = []
        self.failed_uncovered: List[Tuple[int, int, int]] = []
        self.selected: List[Tuple[int, int, int]] = []  # (gen, lo, hi) logical intervals
        self.n_rows = 0


def load(out: str, manifest: dict) -> Loaded:
    """Select a generation per logical interval, load rows, run integrity checks."""
    L = Loaded()
    con = L.con
    con.executescript("""
    CREATE TABLE rows(tx_id TEXT PRIMARY KEY, dialog_id TEXT, org TEXT, resource TEXT, deleted INTEGER,
                      sender_created_at TEXT, n_att INTEGER, cls TEXT, tx_ms INTEGER, month TEXT,
                      lo INTEGER, hi INTEGER, gen INTEGER, observed_at TEXT);
    CREATE TABLE coverage(gen INTEGER, lo INTEGER, hi INTEGER, seconds REAL, observed_at TEXT, n INTEGER);
    CREATE INDEX ix_rows_dialog ON rows(dialog_id);
    CREATE INDEX ix_rows_res ON rows(resource);
    """)
    tz = zoneinfo.ZoneInfo(manifest.get("tz", TZ_NAME))
    gens = list_gens(out)
    if not gens:
        L.invalid.append("no chunk files under %s/chunks" % out)
        return L
    leaves = {g: list_leaves(out, g) for g in gens}
    intervals = logical_intervals(manifest["from_ms"], manifest["to_ms"])
    # Every leaf must lie wholly inside one logical interval and inside the run's
    # bounds. A leaf crossing a boundary can neither be selected nor counted as
    # coverage, so it is invalid data, never a silent omission.
    for g in gens:
        for a, b in leaves[g][0] + leaves[g][1]:
            if b <= a or not any(lo <= a and b <= hi for lo, hi in intervals):
                L.invalid.append("leaf gen%d [%d,%d) crosses a logical interval boundary or the run bounds" % (g, a, b))
    if L.invalid:
        return L
    chosen: List[Tuple[int, int, int, List[Tuple[int, int]]]] = []
    for lo, hi in intervals:
        pick = None
        for g in reversed(gens):
            if tiles(leaves[g][0], lo, hi):
                pick = g
                break
        if pick is None:
            # No generation tiles the interval: keep the one with the most coverage
            # (lowest gen on ties), never mixing generations, and record the gaps.
            pick = max(gens, key=lambda g: (covered_ms(leaves[g][0], lo, hi), -g))
            for a, b in leaves[pick][1]:
                if a < hi and b > lo:
                    L.failed_uncovered.append((pick, a, b))
        # Completeness is judged on exactly the leaves that will be loaded.
        inside = sorted(l for l in leaves[pick][0] if l[0] >= lo and l[1] <= hi)
        for (a1, b1), (a2, b2) in zip(inside, inside[1:]):
            if a2 < b1:
                L.invalid.append("overlapping selected leaves in gen %d: [%d,%d) and [%d,%d)" % (pick, a1, b1, a2, b2))
        cur = lo
        for a, b in inside:
            if a > cur:
                L.gaps.append((cur, a))
            cur = max(cur, b)
        if cur < hi:
            L.gaps.append((cur, hi))
        chosen.append((pick, lo, hi, inside))
        L.selected.append((pick, lo, hi))

    for gen, lo, hi, inside in chosen:
        for a, b in inside:
            p = leaf_path(out, gen, a, b)
            try:
                with gzip.open(p, "rb") as f:
                    payload = json.load(f)
            except Exception as e:  # corrupt or truncated file: invalid, not incomplete
                L.invalid.append("corrupt chunk file %s: %s" % (p, e))
                continue
            if payload.get("lo") != a or payload.get("hi") != b or payload.get("gen") != gen:
                L.invalid.append("chunk file %s does not match its name" % p)
                continue
            if payload.get("query_version") != manifest["query_version"]:
                L.invalid.append("chunk file %s has query version %s, manifest %s" % (p, payload.get("query_version"), manifest["query_version"]))
                continue
            rows = payload["rows"]
            con.execute("INSERT INTO coverage VALUES (?,?,?,?,?,?)", (gen, a, b, payload.get("seconds"), payload.get("observed_at"), len(rows)))
            for r in rows:
                if len(r) != 8 or r[7] not in CLASSES:
                    L.invalid.append("invalid row in %s: %r" % (p, r))
                    continue
                ms = uuid_ms(r[0])
                if not (a <= ms < b):
                    L.invalid.append("row %s in %s lies outside its chunk bounds" % (r[0], p))
                    continue
                month = month_label(ms, tz)
                try:
                    con.execute("INSERT INTO rows VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?)",
                                (r[0], r[1], r[2], r[3], 1 if r[4] == "t" else 0, r[5] or None, int(r[6]), r[7], ms, month,
                                 a, b, gen, payload.get("observed_at")))
                    L.n_rows += 1
                except sqlite3.IntegrityError:
                    L.invalid.append("duplicate transmission id %s (in %s)" % (r[0], p))
    con.commit()
    return L


def print_table(rows: List[Tuple], header: List[str]) -> None:
    widths = [max(len(str(h)), *(len(str(r[i])) for r in rows)) if rows else len(str(h)) for i, h in enumerate(header)]
    print("  " + "  ".join(str(h).ljust(w) for h, w in zip(header, widths)))
    for r in rows:
        print("  " + "  ".join(str(v).ljust(w) for v, w in zip(r, widths)))


def cmd_report(a: argparse.Namespace) -> None:
    out = a.outdir
    manifest = load_manifest(out)
    if manifest is None:
        die("No manifest in %s" % out)
    meta = load_meta(out, manifest, a.rebind_meta)
    L = load(out, manifest)
    con = L.con
    print("Run %s .. %s on %s, query %s, %d rows loaded from %d selected intervals"
          % (manifest["from"], manifest["to"], manifest["env"], manifest["query_version"], L.n_rows, len(L.selected)))
    if L.invalid:
        print("\nINVALID DATA, reporting stopped (%d problems):" % len(L.invalid))
        for m in L.invalid[:30]:
            print("  " + m)
        sys.exit(3)
    complete = not L.gaps and not L.failed_uncovered
    if not complete:
        gap_ms = sum(b - a for a, b in L.gaps)
        print("\nWARNING: INCOMPLETE COVERAGE. %d gap(s) totalling %.1f h, %d failed leaf/leaves not covered. Totals below are partial."
              % (len(L.gaps), gap_ms / HOUR_MS, len(L.failed_uncovered)))
        for a_, b in L.gaps[:10]:
            print("  gap    %s -> %s" % (fmt_ms(a_), fmt_ms(b)))
        for g, a_, b in L.failed_uncovered[:10]:
            print("  failed gen%d %s -> %s" % (g, fmt_ms(a_), fmt_ms(b)))
    else:
        print("Coverage complete: %s .. %s tiled with no gaps or overlaps." % (manifest["from"], manifest["to"]))
    gens_used = sorted({g for g, _, _ in L.selected})
    print("Generations selected: %s. Query time %.0f s over %d leaves."
          % (gens_used, con.execute("SELECT coalesce(sum(seconds),0) FROM coverage").fetchone()[0],
             con.execute("SELECT count(*) FROM coverage").fetchone()[0]))
    if not meta:
        print("NOTE: no meta.csv in %s; app grouping unavailable. Run fetch-meta." % out)

    con.create_function("app_group", 1, lambda r: app_group(r, meta))
    con.execute("CREATE TABLE r AS SELECT *, app_group(resource) AS grp FROM rows")
    con.execute("CREATE INDEX ix_r_grp ON r(grp)")

    print("\n== Classes, all transmissions ==")
    rows = con.execute("SELECT cls, count(*), count(DISTINCT dialog_id) FROM r GROUP BY cls ORDER BY 2 DESC").fetchall()
    print_table(rows, ["class", "transmissions", "distinct dialogs"])

    print("\n== By app group (a3_pdf = Altinn 3 app whose current metadata generates a PDF) ==")
    rows = con.execute("""SELECT grp, count(*), count(DISTINCT dialog_id),
        sum(cls='marker'), sum(cls='empty'), sum(cls='no_marker'), sum(cls='pdf_unknown'), sum(cls='no_pdf_url'), sum(deleted)
        FROM r GROUP BY grp ORDER BY 2 DESC""").fetchall()
    print_table(rows, ["group", "tx", "dialogs", "marker", "empty", "no_marker", "pdf_unknown", "no_pdf_url", "deleted"])

    b = manifest["period_boundary_ms"]
    print("\n== a3_pdf apps by period (boundary %s = adapter #224 in production) ==" % fmt_ms(b))
    rows = con.execute("""SELECT CASE WHEN tx_ms < ? THEN 'before' ELSE 'after' END AS period, count(*), count(DISTINCT dialog_id),
        sum(cls!='marker') AS candidates, count(DISTINCT CASE WHEN cls!='marker' THEN dialog_id END) AS cand_dialogs,
        sum(cls='empty'), sum(cls='no_marker'), sum(cls='pdf_unknown'), sum(cls='no_pdf_url')
        FROM r WHERE grp='a3_pdf' AND deleted=0 GROUP BY 1 ORDER BY 1 DESC""", (b,)).fetchall()
    print_table(rows, ["period", "tx", "dialogs", "candidate tx", "candidate dialogs", "empty", "no_marker", "pdf_unknown", "no_pdf_url"])

    print("\n== a3_pdf apps per month (%s), not deleted ==" % TZ_NAME)
    rows = con.execute("""SELECT month, count(*), count(DISTINCT dialog_id), sum(cls!='marker'),
        count(DISTINCT CASE WHEN cls!='marker' THEN dialog_id END), sum(cls='empty'), sum(cls='no_marker'), sum(cls='pdf_unknown'), sum(cls='no_pdf_url')
        FROM r WHERE grp='a3_pdf' AND deleted=0 GROUP BY 1 ORDER BY 1""").fetchall()
    print_table(rows, ["month", "tx", "dialogs", "candidate tx", "candidate dialogs", "empty", "no_marker", "pdf_unknown", "no_pdf_url"])

    print("\n== Top apps by candidate transmissions (all groups) ==")
    rows = con.execute("""SELECT replace(resource,'urn:altinn:resource:',''), grp, count(*), sum(cls!='marker'),
        count(DISTINCT CASE WHEN cls!='marker' THEN dialog_id END), sum(cls='empty'), sum(cls='no_marker'), sum(cls='pdf_unknown'), sum(cls='no_pdf_url')
        FROM r GROUP BY 1,2 ORDER BY 4 DESC LIMIT 40""").fetchall()
    print_table(rows, ["app", "group", "tx", "candidate tx", "candidate dialogs", "empty", "no_marker", "pdf_unknown", "no_pdf_url"])

    print("\n== Deleted dialogs (all groups), for information ==")
    rows = con.execute("SELECT count(*), count(DISTINCT dialog_id), sum(cls!='marker') FROM r WHERE deleted=1").fetchall()
    print_table(rows, ["tx", "dialogs", "candidate tx"])

    db_path = os.path.join(out, "audit.sqlite")
    if os.path.exists(db_path):
        os.unlink(db_path)
    disk = sqlite3.connect(db_path)
    con.backup(disk)
    disk.close()
    print("\nDetail in %s (tables r, coverage). Status: %s." % (db_path, "SCAN COMPLETE" if complete else "SCAN INCOMPLETE"))


# ----------------------------------------------------------------- export ---
def cmd_export(a: argparse.Namespace) -> None:
    out = a.outdir
    manifest = load_manifest(out)
    if manifest is None:
        die("No manifest in %s" % out)
    meta = load_meta(out, manifest, a.rebind_meta)
    L = load(out, manifest)
    if L.invalid:
        die("INVALID DATA, export refused: %s" % "; ".join(L.invalid[:5]), 3)
    complete = not L.gaps and not L.failed_uncovered
    if not complete and not a.allow_partial:
        die("Coverage incomplete (%d gaps, %d failed leaves). Export refused; pass --allow-partial to export a labelled partial list." % (len(L.gaps), len(L.failed_uncovered)), 3)
    con = L.con
    con.create_function("app_group", 1, lambda r: app_group(r, meta))
    where = "cls != 'marker'"
    scope = {"exclude_no_pdf_apps": bool(a.exclude_no_pdf_apps), "include_deleted": bool(a.include_deleted), "partial": not complete}
    if a.exclude_no_pdf_apps:
        where += " AND app_group(resource) != 'a3_nopdf'"
    if not a.include_deleted:
        where += " AND deleted = 0"
    cand_tx = con.execute("SELECT tx_id, dialog_id, org, resource, cls, n_att, sender_created_at, tx_ms, observed_at, gen, deleted FROM rows WHERE %s ORDER BY dialog_id, tx_ms" % where).fetchall()
    dialogs = sorted({r[1] for r in cand_tx})
    print("%d candidate transmissions in %d distinct dialogs. Looking up storage labels in batches of %d..." % (len(cand_tx), len(dialogs), a.batch))

    db = Db(a.env, a.dsn)
    # The labels must come from the database that was audited; an --env/--dsn
    # override pointing elsewhere would produce wrong mappings or false "missing".
    ident = db.identity()
    if manifest.get("db_identity") and ident != manifest["db_identity"]:
        die("Label lookup refused: connected to %s but the audit ran against %s." % (ident, manifest["db_identity"]), 3)
    labels: Dict[str, List[str]] = {d: [] for d in dialogs}
    for i in range(0, len(dialogs), a.batch):
        batch = dialogs[i:i + a.batch]
        rc, rows, err = db.query(LABEL_SQL.format(ids=",".join(batch)))
        if rc != 0:
            die("Label lookup failed (rc %d): %s" % (rc, err.strip()), 1)
        for did, val in rows:
            labels.setdefault(did, []).append(val)
        time.sleep(0.2)

    # A dialog is mapped only when it has storage labels and ALL of them parse to
    # the same (partyId, instanceGuid). Anything else is unresolved with a reason.
    mapped: Dict[str, Tuple[str, str]] = {}
    unresolved: List[Tuple[str, str, str]] = []
    for d in dialogs:
        vals = labels.get(d, [])
        parsed = set()
        bad = []
        for v in vals:
            m = STORAGE_LABEL_RE.match(v)
            if m:
                parsed.add((m.group("party"), m.group("guid").lower()))
            else:
                bad.append(v)
        if not vals:
            unresolved.append((d, "missing", ""))
        elif bad:
            unresolved.append((d, "malformed", "|".join(vals)))
        elif len(parsed) > 1:
            unresolved.append((d, "ambiguous", "|".join(vals)))
        else:
            mapped[d] = next(iter(parsed))
    if len(mapped) + len(unresolved) != len(dialogs):
        die("Export accounting invariant violated: %d dialogs != %d mapped + %d unresolved" % (len(dialogs), len(mapped), len(unresolved)), 3)

    by_dialog: Dict[str, List[Tuple]] = {}
    for r in cand_tx:
        by_dialog.setdefault(r[1], []).append(r)

    def w(path: str, header: List[str], rows: List[List[str]]) -> None:
        buf = [",".join(header)]
        for r in rows:
            buf.append(",".join('"%s"' % str(v).replace('"', '""') if ("," in str(v) or '"' in str(v)) else str(v) for v in r))
        atomic_write(path, ("\n".join(buf) + "\n").encode())

    # Execution rows are per Storage INSTANCE. Several dialogs can map to one
    # instance; they are grouped, and every dialog and transmission stays in the
    # evidence file.
    by_instance: Dict[Tuple[str, str], List[str]] = {}
    for d, key in mapped.items():
        by_instance.setdefault(key, []).append(d)
    cand_rows = []
    for (party, guid), ds in sorted(by_instance.items()):
        txs = [t for d in sorted(ds) for t in by_dialog[d]]
        cand_rows.append([party, guid, ";".join(sorted(ds)), len(ds), txs[0][2], txs[0][3].replace("urn:altinn:resource:", ""),
                          app_group(txs[0][3], meta), int(any(t[10] for t in txs)), len(txs),
                          ";".join(sorted({t[4] for t in txs})), utc_iso(min(t[7] for t in txs))])
    w(os.path.join(out, "candidates.csv"),
      ["party_id", "instance_guid", "dialog_ids", "n_dialogs", "org", "app", "app_group", "deleted", "n_candidate_tx", "classes", "first_tx_time_utc"], cand_rows)
    w(os.path.join(out, "candidate_transmissions.csv"),
      ["tx_id", "dialog_id", "org", "app", "class", "n_attachments", "sender_created_at", "tx_time_utc", "observed_at", "gen", "deleted"],
      [[r[0], r[1], r[2], r[3].replace("urn:altinn:resource:", ""), r[4], r[5], r[6] or "", utc_iso(r[7]), r[8], r[9], r[10]] for r in cand_tx])
    w(os.path.join(out, "unresolved.csv"), ["dialog_id", "reason", "labels"], [list(u) for u in unresolved])
    meta_p = os.path.join(out, "meta.json")
    export_manifest = {
        "exported_at": utc_iso(), "run": {k: manifest[k] for k in ("env", "from", "to", "query_version", "created_at")},
        "scan_complete": complete, "all_candidates_mapped": not unresolved, "scope": scope,
        "candidate_transmissions": len(cand_tx), "candidate_dialogs": len(dialogs), "mapped_dialogs": len(mapped),
        "candidate_instances": len(cand_rows), "unresolved_dialogs": len(unresolved), "unresolved_by_reason": {k: sum(1 for u in unresolved if u[1] == k) for k in ("missing", "malformed", "ambiguous")},
        "meta_snapshot": json.load(open(meta_p)) if os.path.exists(meta_p) else None,
        "generations_selected": sorted({g for g, _, _ in L.selected}),
    }
    atomic_write(os.path.join(out, "export_manifest.json"), json.dumps(export_manifest, indent=1).encode())
    print(json.dumps({k: v for k, v in export_manifest.items() if k != "meta_snapshot"}, indent=1))
    print("Written: candidates.csv (per instance), candidate_transmissions.csv (evidence), unresolved.csv, export_manifest.json")


# ------------------------------------------------------------------- main ---
def main() -> None:
    p = argparse.ArgumentParser(description=__doc__ or "transmission PDF audit")
    sub = p.add_subparsers(dest="cmd", required=True)

    r = sub.add_parser("run", help="walk the range in chunks (resumable)")
    r.add_argument("env", nargs="?", default=None, help="test|yt01|staging|prod (or use --dsn)")
    r.add_argument("frm", metavar="FROM", help="start, ISO in %s, whole hour" % TZ_NAME)
    r.add_argument("to", metavar="TO", help="end (exclusive), ISO in %s, or 'now' (frozen at first run)" % TZ_NAME)
    r.add_argument("outdir")
    r.add_argument("--dsn", help="libpq connection string instead of ENV (local testing); password from PGPASSWORD")
    r.add_argument("--pause", type=float, default=0.3, help="base pause between statements, seconds")
    r.add_argument("--gen", type=int, default=1, help="generation to write (default 1; --rescan-days picks the next)")
    r.add_argument("--rescan-days", type=int, default=0, help="re-scan only the last N days before TO as a new generation")
    r.add_argument("--max-chunk-ms", type=int, default=HOUR_MS, help="largest leaf to attempt (testing: force smaller leaves)")
    r.set_defaults(fn=cmd_run)

    e = sub.add_parser("explain", help="EXPLAIN (ANALYZE, BUFFERS) of one chunk, bounded by --timeout")
    e.add_argument("env", nargs="?", default=None)
    e.add_argument("frm", metavar="FROM")
    e.add_argument("to", metavar="TO")
    e.add_argument("--dsn")
    e.add_argument("--timeout", default=STMT_TIMEOUT)
    e.set_defaults(fn=cmd_explain)

    m = sub.add_parser("fetch-meta", help="fetch app metadata into OUTDIR/meta.csv")
    m.add_argument("outdir")
    m.add_argument("--force", action="store_true", help="replace an existing meta.csv")
    m.set_defaults(fn=cmd_fetch_meta)

    rp = sub.add_parser("report", help="integrity checks and counts")
    rp.add_argument("outdir")
    rp.add_argument("--rebind-meta", action="store_true", help="bind the current meta.csv to the run even if a different snapshot was bound")
    rp.set_defaults(fn=cmd_report)

    x = sub.add_parser("export", help="backfill candidate list with Storage ids")
    x.add_argument("outdir")
    x.add_argument("--env", default=None, help="for the label lookup (defaults to the run's env)")
    x.add_argument("--dsn")
    x.add_argument("--allow-partial", action="store_true", help="export even with coverage gaps (labelled partial)")
    x.add_argument("--exclude-no-pdf-apps", action="store_true", help="drop apps whose CURRENT metadata has no PDF-generating type (recorded in export_manifest)")
    x.add_argument("--include-deleted", action="store_true", help="include transmissions of deleted dialogs")
    x.add_argument("--rebind-meta", action="store_true", help="bind the current meta.csv to the run even if a different snapshot was bound")
    x.add_argument("--batch", type=int, default=500)
    x.set_defaults(fn=cmd_export)

    a = p.parse_args()
    if a.cmd == "export" and a.env is None and a.dsn is None:
        mf = load_manifest(a.outdir)
        if mf:
            a.env = None if mf.get("env") == "dsn" else mf.get("env")
            a.dsn = mf.get("dsn")
    a.fn(a)


if __name__ == "__main__":
    main()
