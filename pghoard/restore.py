"""
pghoard - list and restore basebackups

Copyright (c) 2016 Ohmu Ltd
See LICENSE for details
"""
from . import common, config, logutil, version
from .patchedtarfile import tarfile
from .postgres_command import PGHOARD_HOST, PGHOARD_PORT
from concurrent import futures
from pghoard.rohmu import compat, get_transfer, IO_BLOCK_SIZE, rohmufile
from pghoard.rohmu.errors import Error, InvalidConfigurationError
from psycopg2.extensions import adapt
from requests import Session
import argparse
import datetime
import dateutil.parser
import io
import json
import logging
import os
import re
import shutil
import statistics
import sys
import tempfile


class RestoreError(Error):
    """Restore error"""


def create_recovery_conf(dirpath, site, *,
                         port=PGHOARD_PORT,
                         primary_conninfo=None,
                         recovery_end_command=None,
                         recovery_target_action=None,
                         recovery_target_name=None,
                         recovery_target_time=None,
                         recovery_target_xid=None,
                         restore_to_master=None):
    restore_command = [
        "pghoard_postgres_command",
        "--mode", "restore",
        "--port", str(port),
        "--site", site,
        "--output", "%p",
        "--xlog", "%f",
    ]
    lines = [
        "# pghoard created recovery.conf",
        "recovery_target_timeline = 'latest'",
        "trigger_file = {}".format(adapt(os.path.join(dirpath, "trigger_file"))),
        "restore_command = '{}'".format(" ".join(restore_command)),
    ]
    if not restore_to_master:
        lines.append("standby_mode = 'on'")
    if primary_conninfo:
        lines.append("primary_conninfo = {}".format(adapt(primary_conninfo)))
    if recovery_end_command:
        lines.append("recovery_end_command = {}".format(adapt(recovery_end_command)))
    if recovery_target_action:
        lines.append("recovery_target_action = '{}'".format(recovery_target_action))
    if recovery_target_name:
        lines.append("recovery_target_name = '{}'".format(recovery_target_name))
    if recovery_target_time:
        lines.append("recovery_target_time = '{}'".format(recovery_target_time))
    if recovery_target_xid:
        lines.append("recovery_target_xid = '{}'".format(recovery_target_xid))
    content = "\n".join(lines) + "\n"
    filepath = os.path.join(dirpath, "recovery.conf")
    filepath_tmp = filepath + ".tmp"
    with open(filepath_tmp, "w") as fp:
        fp.write(content)
    os.rename(filepath_tmp, filepath)
    return content


def print_basebackup_list(basebackups, *, caption="Available basebackups", verbose=True):
    print(caption, "\n")
    fmt = "{name:40}  {size:>11}  {orig_size:>11}  {time:20}".format
    print(fmt(name="Basebackup", size="Backup size", time="Start time", orig_size="Orig size"))
    print(fmt(name="-" * 40, size="-" * 11, time="-" * 20, orig_size="-" * 11))
    for b in sorted(basebackups, key=lambda b: b["name"]):
        meta = b["metadata"].copy()
        lm = meta.pop("start-time")
        if isinstance(lm, str):
            lm = dateutil.parser.parse(lm)
        if lm.tzinfo:  # pylint: disable=no-member
            lm = lm.astimezone(datetime.timezone.utc).replace(tzinfo=None)  # pylint: disable=no-member
        lm_str = lm.isoformat()[:19] + "Z"  # # pylint: disable=no-member
        size_str = "{} MB".format(b["size"] // (1024 ** 2))
        orig_size = int(meta.pop("original-file-size", 0) or 0)
        if orig_size:
            orig_size_str = "{} MB".format(orig_size // (1024 ** 2))
        else:
            orig_size_str = "n/a"
        print(fmt(name=b["name"], size=size_str, time=lm_str, orig_size=orig_size_str))
        if verbose:
            print("    metadata:", meta)


class Restore:
    log_tracebacks = False

    def __init__(self):
        self.config = None
        self.log = logging.getLogger("PGHoardRestore")
        self.storage = None

    def create_parser(self):
        parser = argparse.ArgumentParser()
        parser.add_argument("--version", action='version', help="show program version",
                            version=version.__version__)
        sub = parser.add_subparsers(help="sub-command help")

        def add_cmd(method):
            cp = sub.add_parser(method.__name__.replace("_", "-"), help=method.__doc__)
            cp.set_defaults(func=method)
            return cp

        def generic_args(require_config=True, require_site=False):
            config_path = os.environ.get("PGHOARD_CONFIG")
            cmd.add_argument("-v", "--verbose", help="verbose output", action="store_true")
            if config_path:
                cmd.add_argument("--config", help="pghoard config file", default=config_path)
            else:
                cmd.add_argument("--config", help="pghoard config file", required=require_config)

            cmd.add_argument("--site", help="pghoard site", required=require_site)

        def host_port_args():
            cmd.add_argument("--host", help="pghoard repository host", default=PGHOARD_HOST)
            cmd.add_argument("--port", help="pghoard repository port", default=PGHOARD_PORT)

        def target_args():
            cmd.add_argument("--basebackup", help="pghoard basebackup", default="latest")
            cmd.add_argument("--primary-conninfo", help="replication.conf primary_conninfo", default="")
            cmd.add_argument("--target-dir", help="pghoard restore target 'pgdata' dir", required=True)
            cmd.add_argument("--overwrite", help="overwrite existing target directory",
                             default=False, action="store_true")
            cmd.add_argument("--tablespace-dir", metavar="NAME=DIRECTORY", action="append",
                             help="map the given tablespace to an existing empty directory; "
                                  "this option can be used multiple times to map multiple tablespaces")
            cmd.add_argument("--recovery-end-command", help="PostgreSQL recovery_end_command", metavar="COMMAND")
            cmd.add_argument("--recovery-target-action", help="PostgreSQL recovery_target_action",
                             choices=["pause", "promote", "shutdown"])
            cmd.add_argument("--recovery-target-name", help="PostgreSQL recovery_target_name", metavar="RESTOREPOINT")
            cmd.add_argument("--recovery-target-time", help="PostgreSQL recovery_target_time", metavar="ISO_TIMESTAMP")
            cmd.add_argument("--recovery-target-xid", help="PostgreSQL recovery_target_xid", metavar="XID")
            cmd.add_argument("--restore-to-master", help="Restore the database to a PG master", action="store_true")

        cmd = add_cmd(self.list_basebackups_http)
        host_port_args()
        generic_args(require_config=False, require_site=True)

        cmd = add_cmd(self.list_basebackups)
        generic_args()

        cmd = add_cmd(self.get_basebackup)
        target_args()
        generic_args()

        return parser

    def list_basebackups_http(self, arg):
        """List available basebackups from a HTTP source"""
        self.storage = HTTPRestore(arg.host, arg.port, arg.site)
        self.storage.show_basebackup_list(verbose=arg.verbose)

    def _get_object_storage(self, site, pgdata):
        storage_config = common.get_object_storage_config(self.config, site)
        storage = get_transfer(storage_config)
        return ObjectStore(storage, self.config["path_prefix"], site, pgdata)

    def list_basebackups(self, arg):
        """List basebackups from an object store"""
        self.config = config.read_json_config_file(arg.config, check_commands=False)
        site = config.get_site_from_config(self.config, arg.site)
        self.storage = self._get_object_storage(site, pgdata=None)
        self.storage.show_basebackup_list(verbose=arg.verbose)

    def get_basebackup(self, arg):
        """Download a basebackup from an object store"""
        if not arg.tablespace_dir:
            tablespace_mapping = {}
        else:
            try:
                tablespace_mapping = dict(v.split("=", 1) for v in arg.tablespace_dir)
            except ValueError:
                raise RestoreError("Invalid tablespace mapping {!r}".format(arg.tablespace_dir))

        self.config = config.read_json_config_file(arg.config, check_commands=False)
        site = config.get_site_from_config(self.config, arg.site)
        try:
            self.storage = self._get_object_storage(site, arg.target_dir)
            self._get_basebackup(
                pgdata=arg.target_dir,
                basebackup=arg.basebackup,
                site=site,
                primary_conninfo=arg.primary_conninfo,
                recovery_end_command=arg.recovery_end_command,
                recovery_target_action=arg.recovery_target_action,
                recovery_target_name=arg.recovery_target_name,
                recovery_target_time=arg.recovery_target_time,
                recovery_target_xid=arg.recovery_target_xid,
                restore_to_master=arg.restore_to_master,
                overwrite=arg.overwrite,
                tablespace_mapping=tablespace_mapping,
            )
        except RestoreError:
            raise
        except Exception as ex:
            if self.log_tracebacks:
                self.log.exception("Unexpected _get_basebackup failure")
            raise RestoreError("{}: {}".format(ex.__class__.__name__, ex))

    def _find_nearest_basebackup(self, recovery_target_time=None):
        applicable_basebackups = []

        basebackups = self.storage.list_basebackups()
        for basebackup in basebackups:
            if recovery_target_time:
                backup_start_time = dateutil.parser.parse(basebackup["metadata"]["start-time"])
                if backup_start_time >= recovery_target_time:
                    continue
            applicable_basebackups.append(basebackup)

        if not applicable_basebackups:
            raise RestoreError("No applicable basebackups found, exiting")

        applicable_basebackups.sort(key=lambda basebackup: basebackup["metadata"]["start-time"])
        caption = "Found {} applicable basebackup{}".format(
            len(applicable_basebackups),
            "" if len(applicable_basebackups) == 1 else "s")
        print_basebackup_list(applicable_basebackups, caption=caption)

        selected = applicable_basebackups[-1]["name"]
        print("\nSelecting {!r} for restore".format(selected))
        return selected

    def _extract_pghoard_bb_v1(self, fileobj, pgdata, tablespaces, tar_meta):
        directories = []
        # | in mode to use tarfile's internal stream buffer manager, currently required because our SnappyFile
        # interface doesn't do proper buffering for reads
        with tarfile.open(fileobj=fileobj, mode="r|", bufsize=IO_BLOCK_SIZE) as tar:
            for tarinfo in tar:
                if tarinfo.name == ".pghoard_tar_metadata.json":
                    tar_meta_bytes = tar.extractfile(tarinfo).read()
                    tar_meta = json.loads(tar_meta_bytes.decode("utf-8"))
                    continue

                if tarinfo.name == "pgdata" or tarinfo.name == "tablespaces":
                    continue  # ignore these directory entries
                if tarinfo.name.startswith("pgdata/"):
                    target_name = os.path.join(pgdata, tarinfo.name[7:])
                elif tarinfo.name.startswith("tablespaces/"):
                    tscomponents = tarinfo.name.split("/", 2)
                    tsname = tscomponents[1]
                    tspath = tablespaces[tsname]["path"]

                    if len(tscomponents) == 2 and tarinfo.isdir():
                        # Create tablespace entry
                        assert tar_meta["tablespaces"][tsname]["oid"] == tablespaces[tsname]["oid"]
                        tblspc_dir = os.path.join(pgdata, "pg_tblspc")
                        os.makedirs(tblspc_dir, exist_ok=True)
                        linkname = os.path.join(pgdata, "pg_tblspc", str(tablespaces[tsname]["oid"]))
                        os.symlink(tspath, linkname)
                        directories.append([tspath, tarinfo])
                        continue

                    target_name = os.path.join(tspath, tscomponents[2])
                else:
                    raise Exception("Unrecognized path {!r} in tar".format(tarinfo.name))

                if tarinfo.isdir():
                    directories.append([target_name, tarinfo])
                    compat.makedirs(target_name, exist_ok=True)
                elif tarinfo.isreg():
                    target_dir = os.path.dirname(target_name)
                    if not os.path.exists(target_dir):
                        compat.makedirs(target_dir, exist_ok=True)
                    tar.makefile(tarinfo, target_name)
                    tar.chmod(tarinfo, target_name)
                    tar.utime(tarinfo, target_name)
                elif tarinfo.issym():
                    os.symlink(tarinfo.linkname, target_name)
                else:
                    raise Exception("Unrecognized file type for file {!r} in tar".format(tarinfo.name))

        for target_name, tarinfo in directories:
            tar.chmod(tarinfo, target_name)
            tar.utime(tarinfo, target_name)

    def _extract_basic(self, fileobj, pgdata):
        # | in mode to use tarfile's internal stream buffer manager, currently required because our SnappyFile
        # interface doesn't do proper buffering for reads
        with tarfile.open(fileobj=fileobj, mode="r|", bufsize=IO_BLOCK_SIZE) as tar:
            tar.extractall(pgdata)

    def _get_basebackup(self, pgdata, basebackup, site,
                        primary_conninfo=None,
                        recovery_end_command=None,
                        recovery_target_action=None,
                        recovery_target_name=None,
                        recovery_target_time=None,
                        recovery_target_xid=None,
                        restore_to_master=None,
                        overwrite=False,
                        tablespace_mapping=None):
        targets = [recovery_target_name, recovery_target_time, recovery_target_xid]
        if sum(0 if flag is None else 1 for flag in targets) > 1:
            raise RestoreError("Specify at most one of recovery_target_name, "
                               "recovery_target_time or recovery_target_xid")

        # If basebackup that we want it set as latest, figure out which one it is
        if recovery_target_time:
            try:
                recovery_target_time = dateutil.parser.parse(recovery_target_time)
            except (TypeError, ValueError) as ex:
                raise RestoreError("recovery_target_time {!r}: {}".format(recovery_target_time, ex))
            basebackup = self._find_nearest_basebackup(recovery_target_time)
        elif basebackup == "latest":
            basebackup = self._find_nearest_basebackup()

        # Grab basebackup metadata to make sure it exists and to look up tablespace requirements
        metadata = self.storage.get_basebackup_metadata(basebackup)
        tablespaces = {}

        # Make sure we have a proper place to write the $PGDATA and possible tablespaces
        dirs_to_create = []
        dirs_to_recheck = []
        dirs_to_wipe = []

        if not os.path.exists(pgdata):
            dirs_to_create.append(pgdata)
        elif overwrite:
            dirs_to_create.append(pgdata)
            dirs_to_wipe.append(pgdata)
        elif os.listdir(pgdata) in ([], ["lost+found"]):
            # Allow empty directories as well as ext3/4 mount points to be used, but check that we can write to them
            dirs_to_recheck.append(["$PGDATA", pgdata])
        else:
            raise RestoreError("$PGDATA target directory {!r} exists, is not empty and --overwrite not specified, aborting."
                               .format(pgdata))

        bmeta = None
        if metadata.get("format") == "pghoard-v1-bmeta":
            # "Backup file" is a metadata object, fetch it to get more information
            bmeta_compressed = self.storage.get_file_bytes(basebackup)
            with rohmufile.file_reader(fileobj=io.BytesIO(bmeta_compressed), metadata=metadata,
                                       key_lookup=config.key_lookup_for_site(self.config, site)) as input_obj:
                bmeta_bytes = input_obj.read()

            bmeta = json.loads(bmeta_bytes.decode("utf-8"))
            tablespaces = bmeta["tablespaces"]

            basebackup_data_files = [
                os.path.join(site, "basebackup_chunk", chunk[0])
                for chunk in bmeta["chunks"]
            ]

        elif metadata.get("format") == "pghoard-bb-v1":
            # Tablespace information stored in object store metadata, look it up
            tsmetare = re.compile("^tablespace-name-([0-9]+)$")
            for kw, value in metadata.items():
                match = tsmetare.match(kw)
                if not match:
                    continue
                tsoid = match.group(1)
                tsname = value
                tspath = metadata["tablespace-path-{}".format(tsoid)]
                tablespaces[tsname] = {
                    "oid": int(tsoid),
                    "path": tspath,
                }

            basebackup_data_files = [basebackup]

        else:
            # Object is a raw (encrypted, compressed) basebackup
            basebackup_data_files = [basebackup]

        # Map tablespaces as requested and make sure the directories exist
        for tsname, tsinfo in tablespaces.items():
            tspath = tablespace_mapping.pop(tsname, tsinfo["path"])
            if not os.path.exists(tspath):
                raise RestoreError("Tablespace {!r} target directory {!r} does not exist, aborting."
                                   .format(tsname, tspath))
            if os.listdir(tspath) not in ([], ["lost+found"]):
                # Allow empty directories as well as ext3/4 mount points to be used, but check that we can write to them
                raise RestoreError("Tablespace {!r} target directory {!r} exists but is not empty, aborting."
                                   .format(tsname, tspath))

            tsinfo["path"] = tspath
            print("Using existing empty directory {!r} for tablespace {!r}".format(tspath, tsname))
            dirs_to_recheck.append(["Tablespace {!r}".format(tsname), tspath])

        # We .pop() the elements of tablespace_mapping above - if mappings are given they must all exist or the
        # user probably made a typo with tablespace names, abort in that case.
        if tablespace_mapping:
            raise RestoreError("Tablespace mapping for {} was requested, but the tablespaces are not present in the backup"
                               .format(sorted(tablespace_mapping)))

        # First check that the existing (empty) directories are writable, then possibly wipe any directories as
        # requested by --overwrite and finally create the new dirs
        for diruse, dirname in dirs_to_recheck:
            try:
                tempfile.TemporaryFile(dir=dirname).close()
            except PermissionError:
                raise RestoreError("{} target directory {!r} is empty, but not writable, aborting."
                                   .format(diruse, dirname))

        for dirname in dirs_to_wipe:
            shutil.rmtree(dirname)
        for dirname in dirs_to_create:
            os.makedirs(dirname)
            os.chmod(dirname, 0o700)

        with futures.ThreadPoolExecutor(max_workers=8) as extract_executor:
            extract_jobs = []
            with futures.ThreadPoolExecutor(max_workers=8) as download_executor:
                download_progress_per_file = {basebackup_data_file: 0 for basebackup_data_file in basebackup_data_files}
                download_jobs = []

                def download_progress(end=""):
                    mean_prog = statistics.mean(download_progress_per_file.values())
                    print("\rDownload progress: {:.2%}".format(mean_prog), end=end)

                for basebackup_data_file in basebackup_data_files:
                    def single_download_progress(current_pos, expected_max):
                        download_progress_per_file[basebackup_data_file] = current_pos / expected_max
                        download_progress()

                    download_jobs.append(download_executor.submit(
                        self.download_one_backup,
                        basebackup_data_file=basebackup_data_file,
                        progress_callback=single_download_progress,
                        site=site,
                    ))

                for future in futures.as_completed(download_jobs):
                    if future.exception():
                        self.log.error("Got error from chunk download: %s", future.exception())
                        continue

                    extract_jobs.append(extract_executor.submit(
                        self.extract_one_backup,
                        item=future.result(),
                        bmeta=bmeta,
                        pgdata=pgdata,
                        site=site,
                        tablespaces=tablespaces,
                    ))

                download_progress(end="\n")

            for future in futures.as_completed(extract_jobs):
                if future.exception():
                    self.log.error("Got error from chunk extraction: %s", future.exception())
                    continue
                pass

        create_recovery_conf(
            dirpath=pgdata,
            site=site,
            port=self.config["http_port"],
            primary_conninfo=primary_conninfo,
            recovery_end_command=recovery_end_command,
            recovery_target_action=recovery_target_action,
            recovery_target_name=recovery_target_name,
            recovery_target_time=recovery_target_time,
            recovery_target_xid=recovery_target_xid,
            restore_to_master=restore_to_master,
        )

        print("Basebackup restoration complete.")
        print("You can start PostgreSQL by running pg_ctl -D %s start" % pgdata)
        print("On systemd based systems you can run systemctl start postgresql")
        print("On SYSV Init based systems you can run /etc/init.d/postgresql start")

    def download_one_backup(self, *, basebackup_data_file, progress_callback, site):
        dl_dir = os.path.join(self.config["backup_location"], self.config["path_prefix"], site, "basebackup_incoming")
        compat.makedirs(dl_dir, exist_ok=True)
        tmp = tempfile.NamedTemporaryFile(dir=dl_dir, prefix="basebackup.", suffix=".pghoard")
        metadata = self.storage.get_basebackup_file_to_fileobj(
            basebackup=basebackup_data_file,
            fileobj=tmp,
            progress_callback=progress_callback)
        progress_callback(1, 1)
        self.log.info("Downloaded %r", basebackup_data_file)
        tmp.seek(0)
        return tmp, metadata

    def extract_one_backup(self, *, item, bmeta, pgdata, site, tablespaces):
        tmp, metadata = item
        with tmp:
            with rohmufile.file_reader(fileobj=tmp, metadata=metadata,
                                       key_lookup=config.key_lookup_for_site(self.config, site)) as input_obj:
                if metadata.get("format") in ("pghoard-bb-v1", "pghoard-v1-bdata"):
                    self._extract_pghoard_bb_v1(input_obj, pgdata, tablespaces, bmeta)
                elif not metadata.get("format"):
                    self._extract_basic(input_obj, pgdata)
                else:
                    raise RestoreError("Unrecognized basebackup format {!r}".format(metadata.get("format")))
        self.log.info("Extracted %r", tmp)

    def run(self, args=None):
        parser = self.create_parser()
        args = parser.parse_args(args)
        if not hasattr(args, "func"):
            parser.print_help()
            return 1
        try:
            exit_code = args.func(args)
            return exit_code
        except KeyboardInterrupt:
            print("*** interrupted by keyboard ***")
            return 1


class ObjectStore:
    def __init__(self, storage, path_prefix, site, pgdata):
        self.storage = storage
        self.path_prefix = path_prefix
        self.site = site
        self.pgdata = pgdata
        self.log = logging.getLogger(self.__class__.__name__)

    def list_basebackups(self):
        return self.storage.list_path(os.path.join(self.path_prefix, self.site, "basebackup"))

    def show_basebackup_list(self, verbose=True):
        result = self.list_basebackups()
        caption = "Available %r basebackups:" % self.site
        print_basebackup_list(result, caption=caption, verbose=verbose)

    def get_basebackup_metadata(self, basebackup):
        return self.storage.get_metadata_for_key(basebackup)

    def get_basebackup_file_to_fileobj(self, basebackup, fileobj, *, progress_callback=None):
        return self.storage.get_contents_to_fileobj(basebackup, fileobj, progress_callback=progress_callback)

    def get_file_bytes(self, name):
        return self.storage.get_contents_to_string(name)[0]


class HTTPRestore(ObjectStore):
    def __init__(self, host, port, site, pgdata=None):
        super().__init__(storage=None, path_prefix=None, site=site, pgdata=pgdata)
        self.host = host
        self.port = port
        self.session = Session()

    def _url(self, path):
        return "http://{host}:{port}/{site}/{path}".format(
            host=self.host,
            port=self.port,
            site=self.site,
            path=path)

    def list_basebackups(self):
        response = self.session.get(self._url("basebackup"))
        return response.json()["basebackups"]


def main():
    logutil.configure_logging(level=logging.INFO)
    try:
        restore = Restore()
        return restore.run()
    except (InvalidConfigurationError, RestoreError) as ex:
        print("FATAL: {}: {}".format(ex.__class__.__name__, ex))
        return 1


if __name__ == "__main__":
    sys.exit(main() or 0)
