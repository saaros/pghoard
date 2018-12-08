"""
pghoard - main pghoard daemon

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from contextlib import closing
from pghoard import config, logutil, metrics, version, wal
from pghoard.basebackup import PGBaseBackup
from pghoard.common import (
    create_alert_file,
    extract_pghoard_bb_v2_metadata,
    get_object_storage_config,
    replication_connection_string_and_slot_using_pgpass,
    write_json_file,
)
from pghoard.compressor import CompressorThread
from pghoard.rohmu.inotify import InotifyWatcher
from pghoard.transfer import TransferAgent
from pghoard.receivexlog import PGReceiveXLog
from pghoard.rohmu import dates, get_transfer, rohmufile
from pghoard.rohmu.compat import suppress
from pghoard.rohmu.errors import FileNotFoundFromStorageError, InvalidConfigurationError
from pghoard.webserver import WebServer
from queue import Empty, Queue
import argparse
import datetime
import io
import json
import logging
import multiprocessing
import os
import psycopg2
import random
import signal
import socket
import subprocess
import sys
import time

# Imported this way because WALReceiver requires an unreleased version of psycopg2
try:
    from pghoard.walreceiver import WALReceiver
except ImportError:
    WALReceiver = None


class PGHoard:
    def __init__(self, config_path):
        self.metrics = None
        self.log = logging.getLogger("pghoard")
        self.log_level = None
        self.running = True
        self.config_path = config_path
        self.compression_queue = Queue()
        self.transfer_queue = Queue()
        self.syslog_handler = None
        self.basebackups = {}
        self.basebackups_callbacks = {}
        self.receivexlogs = {}
        self.compressors = []
        self.walreceivers = {}
        self.transfer_agents = []
        self.config = {}
        self.mp_manager = None
        self.site_transfers = {}
        self.state = {
            "backup_sites": {},
            "startup_time": datetime.datetime.utcnow().isoformat(),
        }
        self.load_config()
        if self.config["transfer"]["thread_count"] > 1:
            self.mp_manager = multiprocessing.Manager()

        if not os.path.exists(self.config["backup_location"]):
            os.makedirs(self.config["backup_location"])

        signal.signal(signal.SIGHUP, self.load_config)
        signal.signal(signal.SIGINT, self.quit)
        signal.signal(signal.SIGTERM, self.quit)
        self.time_of_last_backup = {}
        self.time_of_last_backup_check = {}
        self.requested_basebackup_sites = set()

        self.inotify = InotifyWatcher(self.compression_queue)
        self.webserver = WebServer(
            self.config,
            self.requested_basebackup_sites,
            self.compression_queue,
            self.transfer_queue)

        for _ in range(self.config["compression"]["thread_count"]):
            compressor = CompressorThread(
                config_dict=self.config,
                compression_queue=self.compression_queue,
                transfer_queue=self.transfer_queue,
                metrics=self.metrics)
            self.compressors.append(compressor)

        compressor_state = {}  # shared among transfer agents
        for _ in range(self.config["transfer"]["thread_count"]):
            ta = TransferAgent(
                config=self.config,
                compression_queue=self.compression_queue,
                mp_manager=self.mp_manager,
                transfer_queue=self.transfer_queue,
                metrics=self.metrics,
                shared_state_dict=compressor_state)
            self.transfer_agents.append(ta)

        logutil.notify_systemd("READY=1")
        self.log.info("pghoard initialized, own_hostname: %r, cwd: %r", socket.gethostname(), os.getcwd())

    def check_pg_versions_ok(self, site, pg_version_server, command):
        if pg_version_server is None:
            # remote pg version not available, don't create version alert in this case
            return False
        if not pg_version_server:
            self.log.error("pghoard does not support versions earlier than 9.3, found: %r", pg_version_server)
            create_alert_file(self.config, "version_unsupported_error")
            return False
        pg_version_client = self.config["backup_sites"][site][command + "_version"]
        if pg_version_server // 100 != pg_version_client // 100:
            self.log.error("Server version: %r does not match %s version: %r",
                           pg_version_server, self.config[command + "_path"], pg_version_client)
            create_alert_file(self.config, "version_mismatch_error")
            return False
        return True

    def create_basebackup(self, site, connection_info, basebackup_path, callback_queue=None):
        connection_string, _ = replication_connection_string_and_slot_using_pgpass(connection_info)
        pg_version_server = self.check_pg_server_version(connection_string, site)
        if not self.check_pg_versions_ok(site, pg_version_server, "pg_basebackup"):
            if callback_queue:
                callback_queue.put({"success": False})
            return

        thread = PGBaseBackup(
            config=self.config,
            site=site,
            connection_info=connection_info,
            basebackup_path=basebackup_path,
            compression_queue=self.compression_queue,
            transfer_queue=self.transfer_queue,
            callback_queue=callback_queue,
            pg_version_server=pg_version_server,
            metrics=self.metrics)
        thread.start()
        self.basebackups[site] = thread

    def check_pg_server_version(self, connection_string, site):
        if "pg_version" in self.config["backup_sites"][site]:
            return self.config["backup_sites"][site]["pg_version"]

        pg_version = None
        try:
            with closing(psycopg2.connect(connection_string)) as c:
                pg_version = c.server_version  # pylint: disable=no-member
                # Cache pg_version so we don't have to query it again, note that this means that for major
                # version upgrades you want to restart pghoard.
                self.config["backup_sites"][site]["pg_version"] = pg_version
        except psycopg2.OperationalError as ex:
            self.log.warning("%s (%s) connecting to DB at: %r",
                             ex.__class__.__name__, ex, connection_string)
            if "password authentication" in str(ex) or "authentication failed" in str(ex):
                create_alert_file(self.config, "authentication_error")
            else:
                create_alert_file(self.config, "configuration_error")
        except Exception as ex:  # log all errors and return None; pylint: disable=broad-except
            self.log.exception("Problem in getting PG server version")
            self.metrics.unexpected_exception(ex, where="check_pg_server_version")
        return pg_version

    def receivexlog_listener(self, site, connection_info, wal_directory):
        connection_string, slot = replication_connection_string_and_slot_using_pgpass(connection_info)
        pg_version_server = self.check_pg_server_version(connection_string, site)
        if not self.check_pg_versions_ok(site, pg_version_server, "pg_receivexlog"):
            return

        self.inotify.add_watch(wal_directory)
        thread = PGReceiveXLog(
            config=self.config,
            connection_string=connection_string,
            wal_location=wal_directory,
            site=site,
            slot=slot,
            pg_version_server=pg_version_server)
        thread.start()
        self.receivexlogs[site] = thread

    def start_walreceiver(self, site, chosen_backup_node, last_flushed_lsn):
        connection_string, slot = replication_connection_string_and_slot_using_pgpass(chosen_backup_node)
        pg_version_server = self.check_pg_server_version(connection_string, site)
        if not WALReceiver:
            self.log.error("Could not import WALReceiver, incorrect psycopg2 version?")
            return

        thread = WALReceiver(
            config=self.config,
            connection_string=connection_string,
            compression_queue=self.compression_queue,
            replication_slot=slot,
            pg_version_server=pg_version_server,
            site=site,
            last_flushed_lsn=last_flushed_lsn,
            metrics=self.metrics)
        thread.start()
        self.walreceivers[site] = thread

    def create_backup_site_paths(self, site):
        site_path = os.path.join(self.config["backup_location"], self.config["backup_sites"][site]["prefix"])
        xlog_path = os.path.join(site_path, "xlog")
        basebackup_path = os.path.join(site_path, "basebackup")

        paths_to_create = [
            site_path,
            xlog_path,
            xlog_path + "_incoming",
            basebackup_path,
            basebackup_path + "_incoming",
        ]

        for path in paths_to_create:
            if not os.path.exists(path):
                os.makedirs(path)

        return xlog_path, basebackup_path

    def delete_remote_wal_before(self, wal_segment, site, pg_version):
        self.log.info("Starting WAL deletion from: %r before: %r, pg_version: %r",
                      site, wal_segment, pg_version)
        storage = self.site_transfers.get(site)
        valid_timeline = True
        tli, log, seg = wal.name_to_tli_log_seg(wal_segment)
        while True:
            if valid_timeline:
                # Decrement one segment if we're on a valid timeline
                if seg == 0 and log == 0:
                    break
                seg, log = wal.get_previous_wal_on_same_timeline(seg, log, pg_version)
            wal_path = os.path.join(self.config["backup_sites"][site]["prefix"], "xlog",
                                    wal.name_for_tli_log_seg(tli, log, seg))
            self.log.debug("Deleting wal_file: %r", wal_path)
            try:
                storage.delete_key(wal_path)
                valid_timeline = True
            except FileNotFoundFromStorageError:
                if not valid_timeline or tli <= 1:
                    # if we didn't find any WALs to delete on this timeline or we're already at
                    # timeline 1 there's no need or possibility to try older timelines, break.
                    self.log.info("Could not delete wal_file: %r, returning", wal_path)
                    break
                # let's try the same segment number on a previous timeline, but flag that timeline
                # as "invalid" until we're able to delete at least one segment on it.
                valid_timeline = False
                tli -= 1
                self.log.info("Could not delete wal_file: %r, trying the same segment on a previous "
                              "timeline (%s)", wal_path, wal.name_for_tli_log_seg(tli, log, seg))
            except Exception as ex:  # FIXME: don't catch all exceptions; pylint: disable=broad-except
                self.log.exception("Problem deleting: %r", wal_path)
                self.metrics.unexpected_exception(ex, where="delete_remote_wal_before")

    def delete_remote_basebackup(self, site, basebackup, metadata):
        start_time = time.monotonic()
        storage = self.site_transfers.get(site)
        main_backup_key = os.path.join(self.config["backup_sites"][site]["prefix"], "basebackup", basebackup)
        basebackup_data_files = [main_backup_key]

        if metadata.get("format") == "pghoard-bb-v2":
            bmeta_compressed = storage.get_contents_to_string(main_backup_key)[0]
            with rohmufile.file_reader(fileobj=io.BytesIO(bmeta_compressed), metadata=metadata,
                                       key_lookup=config.key_lookup_for_site(self.config, site)) as input_obj:
                bmeta = extract_pghoard_bb_v2_metadata(input_obj)
                self.log.debug("PGHoard chunk metadata: %r", bmeta)
                for chunk in bmeta["chunks"]:
                    basebackup_data_files.append(os.path.join(
                        self.config["backup_sites"][site]["prefix"],
                        "basebackup_chunk",
                        chunk["chunk_filename"],
                    ))

        self.log.debug("Deleting basebackup datafiles: %r", ', '.join(basebackup_data_files))
        for obj_key in basebackup_data_files:
            try:
                storage.delete_key(obj_key)
            except FileNotFoundFromStorageError:
                self.log.info("Tried to delete non-existent basebackup %r", obj_key)
            except Exception as ex:  # FIXME: don't catch all exceptions; pylint: disable=broad-except
                self.log.exception("Problem deleting: %r", obj_key)
                self.metrics.unexpected_exception(ex, where="delete_remote_basebackup")
        self.log.info("Deleted basebackup datafiles: %r, took: %.2fs",
                      ', '.join(basebackup_data_files), time.monotonic() - start_time)

    def get_remote_basebackups_info(self, site):
        storage = self.site_transfers.get(site)
        if not storage:
            storage_config = get_object_storage_config(self.config, site)
            storage = get_transfer(storage_config)
            self.site_transfers[site] = storage

        results = storage.list_path(os.path.join(self.config["backup_sites"][site]["prefix"], "basebackup"))
        for entry in results:
            # drop path from resulting list and convert timestamps
            entry["name"] = os.path.basename(entry["name"])
            entry["metadata"] = {k.lower(): v for k, v in entry["metadata"].items()}
            entry["metadata"]["start-time"] = dates.parse_timestamp(entry["metadata"]["start-time"])

        results.sort(key=lambda entry: entry["metadata"]["start-time"])
        return results

    def check_backup_count_and_state(self, site):
        """Look up basebackups from the object store, prune any extra
        backups and return the datetime of the latest backup."""
        basebackups = self.get_remote_basebackups_info(site)
        self.log.debug("Found %r basebackups", basebackups)

        if basebackups:
            last_backup_time = basebackups[-1]["metadata"]["start-time"]
        else:
            last_backup_time = None

        allowed_basebackup_count = self.config["backup_sites"][site]["basebackup_count"]
        if allowed_basebackup_count is None:
            allowed_basebackup_count = len(basebackups)

        while len(basebackups) > allowed_basebackup_count:
            self.log.warning("Too many basebackups: %d > %d, %r, starting to get rid of %r",
                             len(basebackups), allowed_basebackup_count, basebackups, basebackups[0]["name"])
            basebackup_to_be_deleted = basebackups.pop(0)
            pg_version = basebackup_to_be_deleted["metadata"].get("pg-version")
            last_wal_segment_still_needed = 0
            if basebackups:
                last_wal_segment_still_needed = basebackups[0]["metadata"]["start-wal-segment"]

            if last_wal_segment_still_needed:
                self.delete_remote_wal_before(last_wal_segment_still_needed, site, pg_version)
            self.delete_remote_basebackup(site, basebackup_to_be_deleted["name"], basebackup_to_be_deleted["metadata"])
        self.state["backup_sites"][site]["basebackups"] = basebackups

        return last_backup_time

    def set_state_defaults(self, site):
        if site not in self.state["backup_sites"]:
            self.state["backup_sites"][site] = {"basebackups": []}

    def startup_walk_for_missed_files(self):
        """Check xlog and xlog_incoming directories for files that receivexlog has received but not yet
        compressed as well as the files we have compressed but not yet uploaded and process them."""
        for site in self.config["backup_sites"]:
            compressed_xlog_path, _ = self.create_backup_site_paths(site)
            uncompressed_xlog_path = compressed_xlog_path + "_incoming"

            # Process uncompressed files (ie WAL pg_receivexlog received)
            for filename in os.listdir(uncompressed_xlog_path):
                full_path = os.path.join(uncompressed_xlog_path, filename)
                if not wal.WAL_RE.match(filename) and not wal.TIMELINE_RE.match(filename):
                    self.log.warning("Found invalid file %r from incoming xlog directory", full_path)
                    continue
                compression_event = {
                    "delete_file_after_compression": True,
                    "full_path": full_path,
                    "site": site,
                    "src_path": "{}.partial",
                    "type": "MOVE",
                }
                self.log.debug("Found: %r when starting up, adding to compression queue", compression_event)
                self.compression_queue.put(compression_event)

            # Process compressed files (ie things we've processed but not yet uploaded)
            for filename in os.listdir(compressed_xlog_path):
                if filename.endswith(".metadata"):
                    continue  # silently ignore .metadata files, they're expected and processed below
                full_path = os.path.join(compressed_xlog_path, filename)
                metadata_path = full_path + ".metadata"
                is_xlog = wal.WAL_RE.match(filename)
                is_timeline = wal.TIMELINE_RE.match(filename)
                if not ((is_xlog or is_timeline) and os.path.exists(metadata_path)):
                    self.log.warning("Found invalid file %r from compressed xlog directory", full_path)
                    continue
                with open(metadata_path, "r") as fp:
                    metadata = json.load(fp)

                transfer_event = {
                    "file_size": os.path.getsize(full_path),
                    "filetype": "xlog" if is_xlog else "timeline",
                    "local_path": full_path,
                    "metadata": metadata,
                    "site": site,
                    "type": "UPLOAD",
                }
                self.log.debug("Found: %r when starting up, adding to transfer queue", transfer_event)
                self.transfer_queue.put(transfer_event)

    def start_threads_on_startup(self):
        # Startup threads
        self.inotify.start()
        self.webserver.start()
        for compressor in self.compressors:
            compressor.start()
        for ta in self.transfer_agents:
            ta.start()

    def _cleanup_inactive_receivexlogs(self, site):
        if site in self.receivexlogs:
            if not self.receivexlogs[site].running:
                if self.receivexlogs[site].is_alive():
                    self.receivexlogs[site].join()
                del self.receivexlogs[site]

    def handle_site(self, site, site_config):
        self.set_state_defaults(site)
        xlog_path, basebackup_path = self.create_backup_site_paths(site)

        if not site_config["active"]:
            return  # If a site has been marked inactive, don't bother checking anything

        self._cleanup_inactive_receivexlogs(site)

        chosen_backup_node = random.choice(site_config["nodes"])

        if site not in self.receivexlogs and site not in self.walreceivers:
            if site_config["active_backup_mode"] == "pg_receivexlog":
                self.receivexlog_listener(site, chosen_backup_node, xlog_path + "_incoming")
            elif site_config["active_backup_mode"] == "walreceiver":
                state_file_path = self.config["json_state_file_path"]
                walreceiver_state = {}
                with suppress(FileNotFoundError):
                    with open(state_file_path, "r") as fp:
                        old_state_file = json.load(fp)
                        walreceiver_state = old_state_file.get("walreceivers", {}).get(site, {})
                self.start_walreceiver(
                    site=site,
                    chosen_backup_node=chosen_backup_node,
                    last_flushed_lsn=walreceiver_state.get("last_flushed_lsn"))

        last_check_time = self.time_of_last_backup_check.get(site)
        if not last_check_time or (time.monotonic() - self.time_of_last_backup_check[site]) > 300:
            self.time_of_last_backup[site] = self.check_backup_count_and_state(site)
            self.time_of_last_backup_check[site] = time.monotonic()

        # check if a basebackup is running, or if a basebackup has just completed
        if site in self.basebackups:
            try:
                result = self.basebackups_callbacks[site].get(block=False)
            except Empty:
                # previous basebackup (or its compression and upload) still in progress
                return
            if self.basebackups[site].is_alive():
                self.basebackups[site].join()
            del self.basebackups[site]
            del self.basebackups_callbacks[site]
            self.log.debug("Basebackup has finished for %r: %r", site, result)
            self.time_of_last_backup[site] = self.check_backup_count_and_state(site)
            self.time_of_last_backup_check[site] = time.monotonic()

        new_backup_needed = False
        if site in self.requested_basebackup_sites:
            self.log.info("Creating a new basebackup for %r due to request", site)
            self.requested_basebackup_sites.discard(site)
            new_backup_needed = True
        elif site_config["basebackup_interval_hours"] is None:
            # Basebackups are disabled for this site (but they can still be requested over the API.)
            pass
        elif self.time_of_last_backup.get(site) is None:
            self.log.info("Creating a new basebackup for %r because there are currently none", site)
            new_backup_needed = True
        else:
            now = datetime.datetime.now(datetime.timezone.utc)
            delta_since_last_backup = now - self.time_of_last_backup[site]
            if delta_since_last_backup >= datetime.timedelta(hours=site_config["basebackup_interval_hours"]):
                if "basebackup_schedule" not in site_config:
                    new_backup_needed = True
                elif now.hour in site_config["basebackup_schedule"]["hours"] and \
                        now.day in site_config["basebackup_schedule"]["days_of_month"] and \
                        now.weekday() in site_config["basebackup_schedule"]["days_of_week"]:
                    new_backup_needed = True
                if new_backup_needed:
                    self.log.info("Creating a new basebackup for %r by schedule (%s from previous)",
                                  site, delta_since_last_backup)

        if new_backup_needed and not os.path.exists(self.config["maintenance_mode_file"]):
            self.basebackups_callbacks[site] = Queue()
            self.create_basebackup(site, chosen_backup_node, basebackup_path, self.basebackups_callbacks[site])

    def run(self):
        self.start_threads_on_startup()
        self.startup_walk_for_missed_files()
        while self.running:
            try:
                for site, site_config in self.config["backup_sites"].items():
                    self.handle_site(site, site_config)
                self.write_backup_state_to_json_file()
            except subprocess.CalledProcessError as ex:
                self.log.error("main loop: %s: %s, retrying...", ex.__class__.__name__, ex)
            except Exception as ex:  # pylint: disable=broad-except
                self.log.exception("Unexpected exception in PGHoard main loop")
                self.metrics.unexpected_exception(ex, where="pghoard_run")
            time.sleep(5.0)

    def write_backup_state_to_json_file(self):
        """Periodically write a JSON state file to disk"""
        start_time = time.time()
        state_file_path = self.config["json_state_file_path"]
        self.state["walreceivers"] = {
            key: {"latest_activity": value.latest_activity, "running": value.running,
                  "last_flushed_lsn": value.last_flushed_lsn}
            for key, value in self.walreceivers.items()
        }
        self.state["pg_receivexlogs"] = {
            key: {"latest_activity": value.latest_activity, "running": value.running}
            for key, value in self.receivexlogs.items()
        }
        self.state["pg_basebackups"] = {
            key: {"latest_activity": value.latest_activity, "running": value.running}
            for key, value in self.basebackups.items()
        }
        self.state["compressors"] = [compressor.state for compressor in self.compressors]
        self.state["transfer_agents"] = [ta.state for ta in self.transfer_agents]
        self.state["queues"] = {
            "compression_queue": self.compression_queue.qsize(),
            "transfer_queue": self.transfer_queue.qsize(),
        }
        self.log.debug("Writing JSON state file to %r", state_file_path)
        write_json_file(state_file_path, self.state)
        self.log.debug("Wrote JSON state file to disk, took %.4fs", time.time() - start_time)

    def load_config(self, _signal=None, _frame=None):  # pylint: disable=unused-argument
        self.log.debug("Loading JSON config from: %r, signal: %r", self.config_path, _signal)
        try:
            new_config = config.read_json_config_file(self.config_path)
        except (InvalidConfigurationError, subprocess.CalledProcessError, UnicodeDecodeError) as ex:
            self.log.exception("Invalid config file %r: %s: %s", self.config_path, ex.__class__.__name__, ex)
            # if we were called by a signal handler we'll ignore (and log)
            # the error and hope the user fixes the configuration before
            # restarting pghoard.
            if _signal is not None:
                return
            if isinstance(ex, InvalidConfigurationError):
                raise
            raise InvalidConfigurationError(self.config_path)

        self.config = new_config
        if self.config.get("syslog") and not self.syslog_handler:
            self.syslog_handler = logutil.set_syslog_handler(
                address=self.config.get("syslog_address", "/dev/log"),
                facility=self.config.get("syslog_facility", "local2"),
                logger=logging.getLogger(),
            )
        # NOTE: getLevelName() also converts level names to numbers
        self.log_level = logging.getLevelName(self.config["log_level"])
        try:
            logging.getLogger().setLevel(self.log_level)
        except ValueError:
            self.log.exception("Problem with log_level: %r", self.log_level)

        # Setup monitoring clients
        self.metrics = metrics.Metrics(
            statsd=self.config.get("statsd", None),
            pushgateway=self.config.get("pushgateway", None))

        for thread in self._get_all_threads():
            thread.config = new_config
            thread.site_transfers = {}

        self.log.debug("Loaded config: %r from: %r", self.config, self.config_path)

    def _get_all_threads(self):
        all_threads = []
        if hasattr(self, "webserver"):  # on first config load webserver isn't initialized yet
            all_threads.append(self.webserver)
        all_threads.extend(self.basebackups.values())
        all_threads.extend(self.receivexlogs.values())
        all_threads.extend(self.walreceivers.values())
        all_threads.extend(self.compressors)
        all_threads.extend(self.transfer_agents)
        return all_threads

    def quit(self, _signal=None, _frame=None):  # pylint: disable=unused-argument
        self.log.warning("Quitting, signal: %r", _signal)
        self.running = False
        self.inotify.running = False
        all_threads = self._get_all_threads()
        for t in all_threads:
            t.running = False
        # Write state file in the end so we get the last known state
        self.write_backup_state_to_json_file()
        for t in all_threads:
            if t.is_alive():
                t.join()
        if self.mp_manager:
            self.mp_manager.shutdown()
            self.mp_manager = None


def main(args=None):
    if args is None:
        args = sys.argv[1:]

    parser = argparse.ArgumentParser(
        prog="pghoard",
        description="postgresql automatic backup daemon")
    parser.add_argument("-D", "--debug", help="Enable debug logging", action="store_true")
    parser.add_argument("--version", action="version", help="show program version",
                        version=version.__version__)
    parser.add_argument("-s", "--short-log", help="use non-verbose logging format", action="store_true")
    parser.add_argument("--config", help="configuration file path", default=os.environ.get("PGHOARD_CONFIG"))
    parser.add_argument("config_file", help="configuration file path (for backward compatibility)",
                        nargs="?")
    arg = parser.parse_args(args)

    config_path = arg.config or arg.config_file
    if not config_path:
        print("pghoard: config file path must be given with --config or via env PGHOARD_CONFIG")
        return 1

    if not os.path.exists(config_path):
        print("pghoard: {!r} doesn't exist".format(config_path))
        return 1

    logutil.configure_logging(short_log=arg.short_log, level=logging.DEBUG if arg.debug else logging.INFO)

    multiprocessing.set_start_method("forkserver")

    try:
        pghoard = PGHoard(config_path)
    except InvalidConfigurationError as ex:
        print("pghoard: failed to load config {}: {}".format(config_path, ex))
        return 1

    return pghoard.run()


if __name__ == "__main__":
    sys.exit(main())
