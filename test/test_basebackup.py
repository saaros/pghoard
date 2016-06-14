"""
pghoard - basebackup tests

Copyright (c) 2015 Ohmu Ltd
See LICENSE for details
"""
from .conftest import TestPG
from copy import deepcopy
from pghoard import pgutil, statsd
from pghoard.basebackup import PGBaseBackup
from pghoard.restore import Restore, RestoreError
from pghoard.rohmu.compat import makedirs
from queue import Queue
from subprocess import check_call
import os
import psycopg2
import pytest
import tarfile
import time


Restore.log_tracebacks = True


class TestPGBaseBackup:
    def test_parse_backup_label(self, tmpdir):
        td = str(tmpdir)
        fn = os.path.join(td, "backup.tar")
        with tarfile.open(fn, "w") as tfile:
            with open(os.path.join(td, "backup_label"), "wb") as fp:
                fp.write(b'''\
START WAL LOCATION: 0/4000028 (file 000000010000000000000004)
CHECKPOINT LOCATION: 0/4000060
BACKUP METHOD: streamed
BACKUP FROM: master
START TIME: 2015-02-12 14:07:19 GMT
LABEL: pg_basebackup base backup
''')
            tfile.add(os.path.join(td, "backup_label"), arcname="backup_label")
        pgb = PGBaseBackup(config=None, site="foosite", connection_info=None,
                           basebackup_path=None, compression_queue=None, transfer_queue=None,
                           stats=statsd.StatsClient(host=None))
        start_wal_segment, start_time = pgb.parse_backup_label_in_tar(fn)
        assert start_wal_segment == "000000010000000000000004"
        assert start_time == "2015-02-12T14:07:19+00:00"

    def test_find_files(self, db):
        top1 = os.path.join(db.pgdata, "top1.test")
        top2 = os.path.join(db.pgdata, "top2.test")
        sub1 = os.path.join(db.pgdata, "global", "sub1.test")
        sub2 = os.path.join(db.pgdata, "global", "sub2.test")
        sub3 = os.path.join(db.pgdata, "global", "sub3.test")

        def create_test_files():
            # Create two temporary files on top level and one in global/ that we'll unlink while iterating
            with open(top1, "w") as t1, open(top2, "w") as t2, \
                    open(sub1, "w") as s1, open(sub2, "w") as s2, open(sub3, "w") as s3:
                t1.write("t1\n")
                t2.write("t2\n")
                s1.write("s1\n")
                s2.write("s2\n")
                s3.write("s3\n")

        pgb = PGBaseBackup(config=None, site="foosite", connection_info=None,
                           basebackup_path=None, compression_queue=None, transfer_queue=None,
                           stats=statsd.StatsClient(host=None))
        create_test_files()
        files = pgb.find_files_to_backup(pgdata=db.pgdata, tablespaces={})
        first_file = next(files)
        os.unlink(top1)
        os.unlink(top2)
        os.unlink(sub1)
        os.unlink(sub2)

        # Missing files are not accepted at top level
        with pytest.raises(FileNotFoundError):
            list(files)

        # Recreate test files and unlink just the one from a subdirectory
        create_test_files()
        files = pgb.find_files_to_backup(pgdata=db.pgdata, tablespaces={})
        first_file = next(files)
        os.unlink(sub1)
        # Missing files in sub directories are ok
        ftbu = [first_file] + list(files)

        # Check that missing_ok flag is not set for top-level items
        for bu_path, local_path, missing_ok in ftbu:
            if os.path.dirname(bu_path) == "pgdata":
                assert missing_ok is False, (bu_path, local_path, missing_ok)
            else:
                assert missing_ok is True, (bu_path, local_path, missing_ok)

        # files to backup should include both top level items and two sub-level items
        bunameset = set(item[0] for item in ftbu)
        assert len(bunameset) == len(ftbu)
        assert "pgdata/top1.test" in bunameset
        assert "pgdata/top2.test" in bunameset
        assert "pgdata/global/sub1.test" not in bunameset
        assert "pgdata/global/sub2.test" in bunameset
        assert "pgdata/global/sub3.test" in bunameset

        # Now delete a file on the top level before we have a chance of tarring anything
        os.unlink(top2)

        class FakeTar:
            def __init__(self):
                self.items = []

            def add(self, local_path, *, arcname, recursive):
                assert recursive is False
                self.items.append((local_path, arcname, os.stat(local_path)))

        # This will fail because top-level items may not be missing
        faketar = FakeTar()
        with pytest.raises(FileNotFoundError):
            pgb.write_files_to_tar(files=ftbu, tar=faketar)

        # Recreate test files and unlink just a subdirectory item
        create_test_files()
        os.unlink(sub2)

        # Now adding files should work and we should end up with every file except for sub2 in the archive
        faketar = FakeTar()
        pgb.write_files_to_tar(files=ftbu, tar=faketar)
        arcnameset = set(item[1] for item in faketar.items)
        assert len(arcnameset) == len(faketar.items)
        expected_items = bunameset - {"pgdata/global/sub2.test"}
        assert arcnameset == expected_items
        assert "pgdata/global/sub1.test" not in arcnameset  # not in set of files to backup
        assert "pgdata/global/sub2.test" not in arcnameset  # acceptable loss
        assert "pgdata/global/sub3.test" in arcnameset  # acceptable

        # Add final entries (ie pg_control)
        pgb.write_final_entries_to_tar(pgdata=db.pgdata, tar=faketar)
        arcnameset = set(item[1] for item in faketar.items)
        assert len(arcnameset) == len(faketar.items)
        expected_items = (bunameset | {"pgdata/global/pg_control"}) - {"pgdata/global/sub2.test"}
        assert arcnameset == expected_items

    def _test_create_basebackup(self, capsys, db, pghoard, mode):
        pghoard.create_backup_site_paths(pghoard.test_site)
        basebackup_path = os.path.join(pghoard.config["backup_location"], pghoard.test_site, "basebackup")
        q = Queue()

        pghoard.config["backup_sites"][pghoard.test_site]["basebackup_mode"] = mode
        pghoard.create_basebackup(pghoard.test_site, db.user, basebackup_path, q)
        result = q.get(timeout=60)
        assert result["success"]

        # make sure it shows on the list
        Restore().run([
            "list-basebackups",
            "--config", pghoard.config_path,
            "--site", pghoard.test_site,
            "--verbose",
        ])
        out, _ = capsys.readouterr()
        assert pghoard.test_site in out
        assert "pg-version" in out

    def _test_restore_basebackup(self, db, pghoard, tmpdir):
        backup_out = tmpdir.join("test-restore").strpath
        # Restoring to empty directory works
        os.makedirs(backup_out)
        Restore().run([
            "get-basebackup",
            "--config", pghoard.config_path,
            "--site", pghoard.test_site,
            "--target-dir", backup_out,
        ])
        # Restoring on top of another $PGDATA doesn't
        with pytest.raises(RestoreError) as excinfo:
            Restore().run([
                "get-basebackup",
                "--config", pghoard.config_path,
                "--site", pghoard.test_site,
                "--target-dir", backup_out,
            ])
        assert "--overwrite not specified" in str(excinfo.value)
        # Until we use the --overwrite flag
        Restore().run([
            "get-basebackup",
            "--config", pghoard.config_path,
            "--site", pghoard.test_site,
            "--target-dir", backup_out,
            "--overwrite",
        ])
        check_call([os.path.join(db.pgbin, "pg_controldata"), backup_out])
        # TODO: check that the backup is valid

    def _test_basebackups(self, capsys, db, pghoard, tmpdir, mode):
        self._test_create_basebackup(capsys, db, pghoard, mode)
        self._test_restore_basebackup(db, pghoard, tmpdir)

    def test_basebackups_basic(self, capsys, db, pghoard, tmpdir):
        self._test_basebackups(capsys, db, pghoard, tmpdir, "basic")

    def test_basebackups_local_tar(self, capsys, db, pghoard, tmpdir):
        self._test_basebackups(capsys, db, pghoard, tmpdir, "local-tar")

    def test_basebackups_pipe(self, capsys, db, pghoard, tmpdir):
        self._test_basebackups(capsys, db, pghoard, tmpdir, "pipe")

    def test_basebackups_tablespaces(self, capsys, db, pghoard, tmpdir):
        # Create a test tablespace for this instance, but make sure we drop it at the end of the test as the
        # database we use is shared by all test cases, and tablespaces are a global concept so the test
        # tablespace could interfere with other tests
        tspath = tmpdir.join("extra-ts").strpath
        os.makedirs(tspath)
        conn_str = pgutil.create_connection_string(db.user)
        conn = psycopg2.connect(conn_str)
        conn.autocommit = True
        cursor = conn.cursor()
        cursor.execute("CREATE TABLESPACE tstest LOCATION %s", [tspath])
        r_db, r_conn = None, None
        try:
            cursor.execute("CREATE TABLE tstest (id BIGSERIAL PRIMARY KEY, value BIGINT) TABLESPACE tstest")
            cursor.execute("INSERT INTO tstest (value) SELECT * FROM generate_series(1, 1000)")
            cursor.execute("CHECKPOINT")
            cursor.execute("SELECT oid, pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname = 'tstest'")
            res = cursor.fetchone()
            assert res[1] == tspath

            # Start receivexlog since we want the WALs to be able to restore later on
            xlog_directory = os.path.join(pghoard.config["backup_location"], pghoard.test_site, "xlog_incoming")
            makedirs(xlog_directory, exist_ok=True)
            pghoard.receivexlog_listener(pghoard.test_site, db.user, xlog_directory)
            self._test_create_basebackup(capsys, db, pghoard, "local-tar")
            cursor.execute("SELECT txid_current(), pg_switch_xlog()")

            backup_out = tmpdir.join("test-restore").strpath
            backup_ts_out = tmpdir.join("test-restore-tstest").strpath

            # Tablespaces are extracted to their previous absolute paths by default, but the path must be empty
            # and it isn't as it's still used by the running PG
            with pytest.raises(RestoreError) as excinfo:
                Restore().run([
                    "get-basebackup",
                    "--config", pghoard.config_path,
                    "--site", pghoard.test_site,
                    "--target-dir", backup_out,
                ])
            assert "Tablespace 'tstest' target directory" in str(excinfo.value)
            assert "not empty" in str(excinfo.value)
            # We can't restore tablespaces to non-existent directories either
            with pytest.raises(RestoreError) as excinfo:
                Restore().run([
                    "get-basebackup",
                    "--config", pghoard.config_path,
                    "--site", pghoard.test_site,
                    "--target-dir", backup_out,
                    "--tablespace-dir", "tstest={}".format(backup_ts_out),
                ])
            assert "Tablespace 'tstest' target directory" in str(excinfo.value)
            assert "does not exist" in str(excinfo.value)
            os.makedirs(backup_ts_out)
            # We can't restore if the directory isn't writable
            os.chmod(backup_ts_out, 0o500)
            with pytest.raises(RestoreError) as excinfo:
                Restore().run([
                    "get-basebackup",
                    "--config", pghoard.config_path,
                    "--site", pghoard.test_site,
                    "--target-dir", backup_out,
                    "--tablespace-dir", "tstest={}".format(backup_ts_out),
                ])
            assert "Tablespace 'tstest' target directory" in str(excinfo.value)
            assert "empty, but not writable" in str(excinfo.value)
            os.chmod(backup_ts_out, 0o700)
            # We can't proceed if we request mappings for non-existent tablespaces
            backup_other_out = tmpdir.join("test-restore-other").strpath
            os.makedirs(backup_other_out)
            with pytest.raises(RestoreError) as excinfo:
                Restore().run([
                    "get-basebackup",
                    "--config", pghoard.config_path,
                    "--site", pghoard.test_site,
                    "--target-dir", backup_out,
                    "--tablespace-dir", "tstest={}".format(backup_ts_out),
                    "--tablespace-dir", "other={}".format(backup_other_out),
                ])
            assert "Tablespace mapping for ['other'] was requested, but" in str(excinfo.value)

            # Now, finally, everything should be valid and we can proceed with restore
            Restore().run([
                "get-basebackup",
                "--config", pghoard.config_path,
                "--site", pghoard.test_site,
                "--restore-to-master",
                "--target-dir", backup_out,
                "--tablespace-dir", "tstest={}".format(backup_ts_out),
            ])

            # Adjust the generated recovery.conf to point pghoard_postgres_command to our instance
            new_cmd = "PYTHONPATH={} python3 -m pghoard.postgres_command".format(os.path.dirname(os.path.dirname(__file__)))
            with open(os.path.join(backup_out, "recovery.conf"), "r+") as fp:
                rconf = fp.read()
                rconf = rconf.replace("pghoard_postgres_command", new_cmd)
                fp.seek(0)
                fp.write(rconf)

            r_db = TestPG(backup_out)
            r_db.user = dict(db.user, host=backup_out)
            r_db.run_pg()
            r_conn_str = pgutil.create_connection_string(r_db.user)

            # Wait for PG to start up
            start_time = time.monotonic()
            while True:
                try:
                    r_conn = psycopg2.connect(r_conn_str)
                    break
                except psycopg2.OperationalError as ex:
                    if "starting up" in str(ex):
                        assert time.monotonic() - start_time <= 10
                        time.sleep(1)
                    else:
                        raise

            r_cursor = r_conn.cursor()
            # Make sure the tablespace is defined and points to the right (new) path
            r_cursor.execute("SELECT oid, pg_tablespace_location(oid) FROM pg_tablespace WHERE spcname = 'tstest'")
            r_res = r_cursor.fetchone()
            assert r_res[1] == backup_ts_out

            # We should be able to read from the table in the tablespace and the values should match what we stored before
            r_cursor.execute("SELECT id FROM tstest")
            r_res = r_cursor.fetchall()
            cursor.execute("SELECT id FROM tstest")
            orig_res = cursor.fetchall()
            assert r_res == orig_res

        finally:
            if r_conn:
                r_conn.close()
            if r_db:
                r_db.kill(force=True)
            cursor.execute("DROP TABLE IF EXISTS tstest")
            cursor.execute("DROP TABLESPACE tstest")
            conn.close()

    def test_handle_site(self, pghoard):
        site_config = deepcopy(pghoard.config["backup_sites"][pghoard.test_site])
        site_config["basebackup_interval_hours"] = 1 / 3600
        assert pghoard.basebackups == {}

        # initialize with a single backup
        backup_start = time.monotonic()
        pghoard.handle_site(pghoard.test_site, site_config)
        assert pghoard.test_site in pghoard.basebackups
        # wait for backup to complete and put the event back in so pghoard finds it, too
        pghoard.basebackups_callbacks[pghoard.test_site].put(pghoard.basebackups_callbacks[pghoard.test_site].get())

        # adjust basebackup interval to be slightly longer than what this
        # basebackup took and make sure it's not retriggered
        site_config["basebackup_interval_hours"] = (time.monotonic() - backup_start + 1) / 3600
        pghoard.handle_site(pghoard.test_site, site_config)
        assert pghoard.test_site not in pghoard.basebackups

        # create a new backup now that we have some state
        time.sleep(2)
        backup_start = time.monotonic()
        pghoard.handle_site(pghoard.test_site, site_config)
        assert pghoard.test_site in pghoard.basebackups
        # wait for backup to complete and put the event back in so pghoard finds it, too
        pghoard.basebackups_callbacks[pghoard.test_site].put(pghoard.basebackups_callbacks[pghoard.test_site].get())
        # now call handle_site so it notices the backup has finished (this must not start a new one)
        pghoard.handle_site(pghoard.test_site, site_config)
        assert pghoard.test_site not in pghoard.basebackups
        first_time_of = pghoard.time_of_last_backup[pghoard.test_site]
        first_time_of_check = pghoard.time_of_last_backup_check[pghoard.test_site]

        # reset the timer to something more sensible and make sure we don't trigger any new basebackups
        site_config["basebackup_interval_hours"] = 1
        pghoard.time_of_last_backup_check[pghoard.test_site] = 0
        time.sleep(1)
        pghoard.handle_site(pghoard.test_site, site_config)
        assert pghoard.test_site not in pghoard.basebackups

        second_time_of = pghoard.time_of_last_backup[pghoard.test_site]
        second_time_of_check = pghoard.time_of_last_backup_check[pghoard.test_site]
        assert second_time_of == first_time_of
        assert second_time_of_check > first_time_of_check

        # create another backup by using the triggering mechanism
        pghoard.requested_basebackup_sites.add(pghoard.test_site)
        pghoard.handle_site(pghoard.test_site, site_config)
        assert pghoard.test_site in pghoard.basebackups
        # again, let pghoard notice the backup is done
        pghoard.basebackups_callbacks[pghoard.test_site].put(pghoard.basebackups_callbacks[pghoard.test_site].get())
        pghoard.handle_site(pghoard.test_site, site_config)
        assert pghoard.test_site not in pghoard.basebackups

        third_time_of = pghoard.time_of_last_backup[pghoard.test_site]
        third_time_of_check = pghoard.time_of_last_backup_check[pghoard.test_site]
        assert third_time_of > second_time_of
        assert third_time_of_check > second_time_of_check

        # call handle_site yet again - nothing should happen and no timestamps should be updated
        time.sleep(1)
        pghoard.handle_site(pghoard.test_site, site_config)
        assert pghoard.test_site not in pghoard.basebackups

        fourth_time_of = pghoard.time_of_last_backup[pghoard.test_site]
        fourth_time_of_check = pghoard.time_of_last_backup_check[pghoard.test_site]
        assert fourth_time_of == third_time_of
        assert fourth_time_of_check == third_time_of_check

        pghoard.write_backup_state_to_json_file()
