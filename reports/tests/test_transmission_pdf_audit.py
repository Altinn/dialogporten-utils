#!/usr/bin/env python3
# =========================================================================
# Local verification for transmission-pdf-audit.py against a local Postgres
# with the Dialogporten schema (the docker-compose database is fine).
#
# Exercises the guarantees a backfill depends on: classification, distinct
# dialog counting, resume after an interrupted chunk write, generation
# selection with a differently split re-scan, duplicate rows, corrupt files,
# unresolved labels and the export accounting invariant.
#
# Usage
#   PGPASSWORD=... python3 reports/tests/test_transmission_pdf_audit.py
#   AUDIT_TEST_DSN overrides the connection (default: local compose db).
#
# The fixture lives in the year 2031 so it cannot collide with seeded data,
# and it is removed again in tearDownClass.
# =========================================================================
import csv
import datetime as dt
import gzip
import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid

HERE = os.path.dirname(os.path.abspath(__file__))
SCRIPT = os.path.join(HERE, "..", "transmission-pdf-audit.py")
DSN = os.environ.get("AUDIT_TEST_DSN", "host=localhost port=15432 dbname=dialogporten user=postgres")

spec = importlib.util.spec_from_file_location("audit", SCRIPT)
audit = importlib.util.module_from_spec(spec)
spec.loader.exec_module(audit)

TZ = audit.TZ
FROM = "2031-01-01T00:00"
TO = "2031-01-01T06:00"
FROM_MS = audit.parse_local(FROM)
HOUR = audit.HOUR_MS


def psql(sql: str) -> str:
    r = subprocess.run(["psql", DSN, "-X", "-q", "-At", "-F", "\t", "-v", "ON_ERROR_STOP=1", "-c", sql],
                       capture_output=True, text=True)
    if r.returncode != 0:
        raise RuntimeError(r.stderr)
    return r.stdout


def v7(ms: int) -> str:
    h = "%012x" % ms
    tail = uuid.uuid4().hex
    return "%s-%s-7%s-8%s-%s" % (h[:8], h[8:12], tail[:3], tail[3:6], tail[6:18])


def ts(ms: int) -> str:
    return dt.datetime.fromtimestamp(ms / 1000, dt.timezone.utc).isoformat()


class Fixture:
    """Inserts transmissions/attachments/actors/labels for existing local dialogs."""

    def __init__(self):
        self.tx, self.att, self.urls, self.actors, self.labels = [], [], [], [], []
        # Seeded dialogs carry non-storage labels; only storage labels matter to the export.
        rows = psql("""SELECT d."Id" FROM "Dialog" d
            WHERE d."Deleted" = false
              AND NOT EXISTS (SELECT 1 FROM "DialogServiceOwnerLabel" l WHERE l."DialogServiceOwnerContextId" = d."Id"
                              AND l."Value" LIKE 'urn:altinn:integration:storage:%')
              AND NOT EXISTS (SELECT 1 FROM "DialogTransmission" t WHERE t."DialogId" = d."Id")
            ORDER BY d."Id" LIMIT 8""").split()
        if len(rows) < 8:
            raise RuntimeError("need 8 local dialogs without storage labels/transmissions, found %d" % len(rows))
        self.d = rows  # D1..D8 -> index 0..7

    def add_tx(self, dialog: str, hour: int, offset_min: int = 10, sender: bool = True) -> str:
        ms = FROM_MS + hour * HOUR + offset_min * 60 * 1000
        tid = v7(ms)
        self.tx.append((tid, ms, dialog))
        if sender:
            self.actors.append((str(uuid.uuid4()), tid, ms + 4000))
        return tid

    def add_att(self, tid: str, name, media=None) -> str:
        aid = str(uuid.uuid4())
        self.att.append((aid, tid, name))
        if media is not None:
            self.urls.append((str(uuid.uuid4()), aid, media))
        return aid

    def add_label(self, dialog: str, value: str) -> None:
        self.labels.append((dialog, value))

    def insert(self) -> None:
        stmts = []
        for tid, ms, d in self.tx:
            stmts.append("""INSERT INTO "DialogTransmission" ("Id","CreatedAt","TypeId","DialogId") VALUES ('%s','%s',7,'%s')""" % (tid, ts(ms), d))
        for aid, tid, name in self.att:
            n = "NULL" if name is None else "'%s'" % name
            stmts.append("""INSERT INTO "Attachment" ("Id","CreatedAt","UpdatedAt","Discriminator","TransmissionId","Name") VALUES ('%s',now(),now(),'DialogTransmissionAttachment','%s',%s)""" % (aid, tid, n))
        for uid, aid, media in self.urls:
            stmts.append("""INSERT INTO "AttachmentUrl" ("Id","CreatedAt","UpdatedAt","MediaType","Url","ConsumerTypeId","AttachmentId") VALUES ('%s',now(),now(),'%s','https://example.invalid/%s',1,'%s')""" % (uid, media, uid, aid))
        for aid, tid, ms in self.actors:
            stmts.append("""INSERT INTO "Actor" ("Id","ActorTypeId","Discriminator","TransmissionId","CreatedAt","UpdatedAt") VALUES ('%s',1,'DialogTransmissionSenderActor','%s','%s','%s')""" % (aid, tid, ts(ms), ts(ms)))
        for d, val in self.labels:
            stmts.append("""INSERT INTO "DialogServiceOwnerLabel" ("Value","DialogServiceOwnerContextId","CreatedAt") VALUES ('%s','%s',now())""" % (val, d))
        psql("BEGIN;\n" + ";\n".join(stmts) + ";\nCOMMIT;")

    def cleanup(self) -> None:
        stmts = []
        for uid, _, _ in self.urls:
            stmts.append("""DELETE FROM "AttachmentUrl" WHERE "Id"='%s'""" % uid)
        for aid, _, _ in self.att:
            stmts.append("""DELETE FROM "Attachment" WHERE "Id"='%s'""" % aid)
        for aid, _, _ in self.actors:
            stmts.append("""DELETE FROM "Actor" WHERE "Id"='%s'""" % aid)
        for tid, _, _ in self.tx:
            stmts.append("""DELETE FROM "DialogTransmission" WHERE "Id"='%s'""" % tid)
        for d, val in self.labels:
            stmts.append("""DELETE FROM "DialogServiceOwnerLabel" WHERE "DialogServiceOwnerContextId"='%s' AND "Value"='%s'""" % (d, val))
        psql("BEGIN;\n" + ";\n".join(stmts) + ";\nCOMMIT;")


def run_cli(*args, expect_rc=0):
    r = subprocess.run([sys.executable, SCRIPT, *args], capture_output=True, text=True)
    if r.returncode != expect_rc:
        raise AssertionError("rc %d != %d\nstdout:\n%s\nstderr:\n%s" % (r.returncode, expect_rc, r.stdout, r.stderr))
    return r.stdout + r.stderr


class AuditTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.fx = f = Fixture()
        D = f.d
        # D1 marker (hour 0)
        t = f.add_tx(D[0], 0); f.add_att(t, "Hovedskjema", "application/xml"); f.add_att(t, "ref-data-as-pdf", "application/pdf")
        # D2 no_marker (hour 0)
        t = f.add_tx(D[1], 0, 20); f.add_att(t, "Hovedskjema", "application/xml"); f.add_att(t, "signature", "application/json")
        # D3 empty (hour 1), no sender actor
        f.add_tx(D[2], 1, sender=False)
        # D4 pdf_unknown (hour 1): null name, pdf url
        t = f.add_tx(D[3], 1, 30); f.add_att(t, None, "application/pdf")
        # D5 no_pdf_url (hour 2): null name, xml url only; plus a named one to prove mixed names go to the URL branch
        t = f.add_tx(D[4], 2); f.add_att(t, None, "application/xml"); f.add_att(t, "Hovedskjema", "application/xml")
        # D6 two transmissions in different hours (distinct-dialog counting)
        t = f.add_tx(D[5], 2, 40); f.add_att(t, "Hovedskjema")
        f.add_tx(D[5], 4)
        # D7 no_marker with two differing labels -> ambiguous
        t = f.add_tx(D[6], 3); f.add_att(t, "Hovedskjema")
        # D8 no_marker with no label -> missing
        t = f.add_tx(D[7], 3, 50); f.add_att(t, "Hovedskjema")
        for i in (0, 1, 2, 3, 4, 5):
            f.add_label(D[i], "urn:altinn:integration:storage:5000%d/%s" % (i, uuid.uuid4()))
        f.add_label(D[6], "urn:altinn:integration:storage:50006/%s" % uuid.uuid4())
        f.add_label(D[6], "urn:altinn:integration:storage:50007/%s" % uuid.uuid4())
        f.insert()
        cls.out = tempfile.mkdtemp(prefix="audit-test-")
        cls.env = dict(os.environ)

    @classmethod
    def tearDownClass(cls):
        cls.fx.cleanup()
        shutil.rmtree(cls.out, ignore_errors=True)

    def _load(self):
        return audit.load(self.out, audit.load_manifest(self.out))

    def test_01_run_and_classify(self):
        out = run_cli("run", "--dsn", DSN, FROM, TO, self.out, "--pause", "0")
        self.assertIn("Done.", out)
        ok, failed = audit.list_leaves(self.out, 1)
        self.assertEqual(len(ok), 6)
        self.assertEqual(failed, [])
        L = self._load()
        self.assertEqual(L.invalid, [])
        self.assertEqual(L.gaps, [])
        self.assertEqual(L.n_rows, 9)
        cls = dict(L.con.execute("SELECT cls, count(*) FROM rows GROUP BY cls").fetchall())
        self.assertEqual(cls, {"marker": 1, "no_marker": 4, "empty": 2, "pdf_unknown": 1, "no_pdf_url": 1})
        self.assertEqual(L.con.execute("SELECT count(DISTINCT dialog_id) FROM rows").fetchone()[0], 8)
        # D6 has two transmissions in two hours: one dialog, two rows
        self.assertEqual(L.con.execute("SELECT count(*) FROM rows WHERE dialog_id=?", (self.fx.d[5],)).fetchone()[0], 2)
        # D3 has no sender actor: kept, with null sender_created_at
        self.assertEqual(L.con.execute("SELECT sender_created_at FROM rows WHERE dialog_id=?", (self.fx.d[2],)).fetchone()[0], None)
        rep = run_cli("report", self.out)
        self.assertIn("Coverage complete", rep)
        self.assertIn("SCAN COMPLETE", rep)

    def test_02_resume_skips_covered_and_refuses_mismatch(self):
        out = run_cli("run", "--dsn", DSN, FROM, TO, self.out, "--pause", "0")
        self.assertNotIn("  ok ", out)  # everything covered, nothing re-run
        self.assertIn("Resuming", out)
        run_cli("run", "--dsn", DSN, "2031-01-01T01:00", TO, self.out, expect_rc=2)  # different FROM

    def test_03_interrupted_write_and_corrupt_file(self):
        gen1 = audit.chunk_dir(self.out, 1)
        # A crash mid-write leaves only a temp file; it must be ignored and the leaf re-run.
        target = audit.leaf_path(self.out, 1, FROM_MS + 5 * HOUR, FROM_MS + 6 * HOUR)
        os.unlink(target)
        with open(os.path.join(gen1, ".tmp-crash"), "wb") as f:
            f.write(b"garbage")
        L = self._load()
        self.assertEqual(L.invalid, [])
        self.assertEqual(L.gaps, [(FROM_MS + 5 * HOUR, FROM_MS + 6 * HOUR)])  # incomplete, not invalid
        rep = run_cli("report", self.out)
        self.assertIn("INCOMPLETE COVERAGE", rep)
        run_cli("export", self.out, "--dsn", DSN, expect_rc=3)  # refused without --allow-partial
        out = run_cli("run", "--dsn", DSN, FROM, TO, self.out, "--pause", "0")
        self.assertIn("  ok ", out)
        self.assertTrue(os.path.exists(target))
        os.unlink(os.path.join(gen1, ".tmp-crash"))
        # A truncated file under a valid leaf name is invalid data and must stop reporting.
        with open(target, "wb") as f:
            f.write(gzip.compress(b'{"lo": 1, "rows": ['))
        L = self._load()
        self.assertTrue(any("corrupt" in m for m in L.invalid))
        run_cli("report", self.out, expect_rc=3)
        os.unlink(target)
        run_cli("run", "--dsn", DSN, FROM, TO, self.out, "--pause", "0")
        self.assertEqual(self._load().invalid, [])

    def test_04_rescan_generation_selection(self):
        # Re-scan the whole range as gen 2 with half-hour leaves.
        out = run_cli("run", "--dsn", DSN, FROM, TO, self.out, "--pause", "0", "--rescan-days", "1", "--max-chunk-ms", str(HOUR // 2))
        self.assertIn("generation 2", out)
        ok2, _ = audit.list_leaves(self.out, 2)
        self.assertEqual(len(ok2), 12)
        L = self._load()
        self.assertEqual(L.invalid, [])
        self.assertEqual({g for g, _, _ in L.selected}, {2})
        self.assertEqual(L.n_rows, 9)
        # Remove one half-hour leaf of hour 3: that logical interval must fall back to gen 1 entirely.
        os.unlink(audit.leaf_path(self.out, 2, FROM_MS + 3 * HOUR, FROM_MS + 3 * HOUR + HOUR // 2))
        L = self._load()
        self.assertEqual(L.invalid, [])
        self.assertEqual(L.gaps, [])
        sel = {(lo - FROM_MS) // HOUR: g for g, lo, hi in L.selected}
        self.assertEqual(sel, {0: 2, 1: 2, 2: 2, 3: 1, 4: 2, 5: 2})
        self.assertEqual(L.n_rows, 9)
        gens = L.con.execute("SELECT DISTINCT gen FROM rows WHERE lo >= ? AND hi <= ?", (FROM_MS + 3 * HOUR, FROM_MS + 4 * HOUR)).fetchall()
        self.assertEqual(gens, [(1,)])

    def test_05_duplicate_and_out_of_bounds_rows_are_invalid(self):
        p = audit.leaf_path(self.out, 1, FROM_MS, FROM_MS + HOUR)
        with gzip.open(p, "rb") as f:
            payload = json.load(f)
        backup = gzip.compress(json.dumps(payload).encode())
        try:
            dup = dict(payload); dup["rows"] = payload["rows"] + [payload["rows"][0]]
            with open(p, "wb") as f:
                f.write(gzip.compress(json.dumps(dup).encode()))
            # gen1 hour 0 is not selected while gen2 tiles it; drop gen2's leaves for hour 0 to select gen1.
            for lo in (FROM_MS, FROM_MS + HOUR // 2):
                os.unlink(audit.leaf_path(self.out, 2, lo, lo + HOUR // 2))
            L = self._load()
            self.assertTrue(any("duplicate transmission id" in m for m in L.invalid), L.invalid)
            oob = dict(payload)
            row = list(payload["rows"][0]); row[0] = v7(FROM_MS + 3 * HOUR)  # a time outside hour 0
            oob["rows"] = payload["rows"] + [row]
            with open(p, "wb") as f:
                f.write(gzip.compress(json.dumps(oob).encode()))
            L = self._load()
            self.assertTrue(any("outside its chunk bounds" in m for m in L.invalid), L.invalid)
            bad = dict(payload)
            row = list(payload["rows"][0]); row[0] = v7(FROM_MS + 1000); row[7] = "weird"
            bad["rows"] = payload["rows"] + [row]
            with open(p, "wb") as f:
                f.write(gzip.compress(json.dumps(bad).encode()))
            self.assertTrue(any("invalid row" in m for m in self._load().invalid))
        finally:
            with open(p, "wb") as f:
                f.write(backup)
        self.assertEqual(self._load().invalid, [])

    def test_06_export_invariant_and_unresolved(self):
        out = run_cli("export", self.out, "--dsn", DSN)
        with open(os.path.join(self.out, "export_manifest.json")) as f:
            m = json.load(f)
        self.assertTrue(m["scan_complete"])
        self.assertFalse(m["all_candidates_mapped"])
        # 8 candidate transmissions (all but D1's marker) in 7 dialogs
        self.assertEqual(m["candidate_transmissions"], 8)
        self.assertEqual(m["candidate_dialogs"], 7)
        self.assertEqual(m["mapped_dialogs"] + m["unresolved_dialogs"], m["candidate_dialogs"])
        self.assertEqual(m["unresolved_by_reason"], {"missing": 1, "malformed": 0, "ambiguous": 1})
        with open(os.path.join(self.out, "unresolved.csv")) as f:
            unresolved = {r["dialog_id"]: r["reason"] for r in csv.DictReader(f)}
        self.assertEqual(unresolved, {self.fx.d[6]: "ambiguous", self.fx.d[7]: "missing"})
        with open(os.path.join(self.out, "candidates.csv")) as f:
            cands = {r["dialog_id"]: r for r in csv.DictReader(f)}
        self.assertNotIn(self.fx.d[0], cands)  # marker dialog is not a candidate
        self.assertEqual(cands[self.fx.d[5]]["n_candidate_tx"], "2")
        self.assertEqual(cands[self.fx.d[1]]["party_id"], "50001")
        self.assertEqual(len(cands), 5)
        # Partial export must be labelled.
        os.unlink(audit.leaf_path(self.out, 1, FROM_MS + 5 * HOUR, FROM_MS + 6 * HOUR))
        for lo in (FROM_MS + 5 * HOUR, FROM_MS + 5 * HOUR + HOUR // 2):
            p = audit.leaf_path(self.out, 2, lo, lo + HOUR // 2)
            if os.path.exists(p):
                os.unlink(p)
        run_cli("export", self.out, "--dsn", DSN, expect_rc=3)
        run_cli("export", self.out, "--dsn", DSN, "--allow-partial")
        with open(os.path.join(self.out, "export_manifest.json")) as f:
            m = json.load(f)
        self.assertFalse(m["scan_complete"])
        self.assertTrue(m["scope"]["partial"])


if __name__ == "__main__":
    unittest.main(verbosity=2)
