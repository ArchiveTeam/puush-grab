from distutils.version import StrictVersion
from seesaw.config import realize, NumberConfigValue
from seesaw.externalprocess import WgetDownload
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.task import SimpleTask, LimitConcurrent, ConditionalTask
from seesaw.tracker import (TrackerRequest, PrepareStatsForTracker,
    UploadWithTracker, SendDoneToTracker, GetItemFromTracker)
from seesaw.util import find_executable
from tornado.ioloop import IOLoop, PeriodicCallback
import datetime
import fcntl
import functools
import json
import os
import pty
import random
import seesaw
import seesaw.externalprocess
import shutil
import subprocess
import time
import urllib2

# check the seesaw version before importing any other components
if StrictVersion(seesaw.__version__) < StrictVersion("0.0.15"):
    raise Exception("This pipeline needs seesaw version 0.0.15 or higher.")


# # Begin AsyncPopen fix

class AsyncPopenFixed(seesaw.externalprocess.AsyncPopen):
    """
    Start the wait_callback after setting self.pipe, to prevent an infinite spew of
    "AttributeError: 'AsyncPopen' object has no attribute 'pipe'"
    """
    def run(self):
        self.ioloop = IOLoop.instance()
        (master_fd, slave_fd) = pty.openpty()

        # make stdout, stderr non-blocking
        fcntl.fcntl(master_fd, fcntl.F_SETFL, fcntl.fcntl(master_fd, fcntl.F_GETFL) | os.O_NONBLOCK)

        self.master_fd = master_fd
        self.master = os.fdopen(master_fd)

        # listen to stdout, stderr
        self.ioloop.add_handler(master_fd, self._handle_subprocess_stdout, self.ioloop.READ)

        slave = os.fdopen(slave_fd)
        self.kwargs["stdout"] = slave
        self.kwargs["stderr"] = slave
        self.kwargs["close_fds"] = True
        self.pipe = subprocess.Popen(*self.args, **self.kwargs)

        self.stdin = self.pipe.stdin

        # check for process exit
        self.wait_callback = PeriodicCallback(self._wait_for_end, 250)
        self.wait_callback.start()

seesaw.externalprocess.AsyncPopen = AsyncPopenFixed

# # End AsyncPopen fix


###########################################################################
# Find a useful Wget+Lua executable.
#
# WGET_LUA will be set to the first path that
# 1. does not crash with --version, and
# 2. prints the required version string
WGET_LUA = find_executable(
    "Wget+Lua",
    ["GNU Wget 1.14.lua.20130523-9a5c"],
    [
        "./wget-lua",
        "./wget-lua-warrior",
        "./wget-lua-local",
        "../wget-lua",
        "../../wget-lua",
        "/home/warrior/wget-lua",
        "/usr/bin/wget-lua"
    ]
)

if not WGET_LUA:
    raise Exception("No usable Wget+Lua found.")


###########################################################################
# The version number of this pipeline definition.
#
# Update this each time you make a non-cosmetic change.
# It will be added to the WARC files and reported to the tracker.
VERSION = "20130801.02"
USER_AGENT = "ArchiveTeam"
# TRACKER_ID = 'test1'
# TRACKER_HOST = 'localhost:8030'
TRACKER_ID = 'puush'
TRACKER_HOST = 'tracker.archiveteam.org'

# these must match from the lua script
EXIT_STATUS_PERMISSION_DENIED = 100
EXIT_STATUS_NOT_FOUND = 101
EXIT_STATUS_OTHER_ERROR = 102


###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.


class ExtraItemParams(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'ExtraItemParams')

    def process(self, item):
        item['wget_exit_status'] = None


class PrepareDirectories(SimpleTask):
    """
      A task that creates temporary directories and initializes filenames.

      It initializes these directories, based on the previously set item_name:
        item["item_dir"] = "%{data_dir}/%{item_name}"
        item["warc_file_base"] = "%{warc_prefix}-%{item_name}-%{timestamp}"

      These attributes are used in the following tasks, e.g., the Wget call.

      * set warc_prefix to the project name.
      * item["data_dir"] is set by the environment: it points to a working
        directory reserved for this item.
      * use item["item_dir"] for temporary files
      """
    def __init__(self, warc_prefix):
        SimpleTask.__init__(self, "PrepareDirectories")
        self.warc_prefix = warc_prefix

    def process(self, item):
        dirname = "/".join((item["data_dir"], item["item_name"]))

        if os.path.isdir(dirname):
            shutil.rmtree(dirname)
        os.makedirs(dirname)

        item["item_dir"] = dirname
        item["warc_file_base"] = "%s-%s-%s" % (
            self.warc_prefix, item["item_name"],
            time.strftime("%Y%m%d-%H%M%S"))

        open("%(item_dir)s/%(warc_file_base)s.warc.gz" % item, "w").close()


class WgetDownloadSaveExitStatus(WgetDownload):
    def handle_process_result(self, exit_code, item):
        item['wget_exit_status'] = exit_code
        WgetDownload.handle_process_result(self, exit_code, item)

    def handle_process_error(self, exit_code, item):
        if exit_code == EXIT_STATUS_OTHER_ERROR:
            delay_seconds = random.randint(60 * 4, 60 * 6)
            item.log_output('Unexpected response from server. '
                'Waiting for %d seconds before continuing...' % delay_seconds)
            IOLoop.instance().add_timeout(
                datetime.timedelta(seconds=delay_seconds),
                functools.partial(WgetDownload.handle_process_error,
                    self, exit_code, item))
        else:
            WgetDownload.handle_process_error(self, exit_code, item)


class MoveFiles(SimpleTask):
    """
      After downloading, this task moves the warc file from the
      item["item_dir"] directory to the item["data_dir"]
      """
    def __init__(self):
        SimpleTask.__init__(self, "MoveFiles")

    def process(self, item):
        os.rename("%(item_dir)s/%(warc_file_base)s.warc.gz" % item,
                "%(data_dir)s/%(warc_file_base)s.warc.gz" % item)


class CleanUpItemDir(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "CleanUpItemDir")

    def process(self, item):
        shutil.rmtree("%(item_dir)s" % item)


def prepare_stats_id_function(item):
    return json.dumps({"wget_exit_status": item["wget_exit_status"]})


def is_wget_exit_ok(item):
    return not item['wget_exit_status']


###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title="Puush",
    project_html="""
    <img class="project-logo" alt="" src="http://archiveteam.org/images/b/b2/Puush_logo.png" />
    <h2>Puush <span class="links"><a href="http://puush.me/">Website</a> &middot; <a href="http://%s/%s/">Leaderboard</a></span></h2>
    <p><b>Puush</b> adds expiry dates to their files.</p>
    """ % (TRACKER_HOST, TRACKER_ID)
    # , utc_deadline = datetime.datetime(2013,08,01, 00,00,1)
)

pipeline = Pipeline(
    GetItemFromTracker("http://%s/%s" % (TRACKER_HOST, TRACKER_ID), downloader, VERSION),
    ExtraItemParams(),
    PrepareDirectories(warc_prefix="puush"),
    WgetDownloadSaveExitStatus([ WGET_LUA,
          "-U", USER_AGENT,
          "-nv",
          "-o", ItemInterpolation("%(item_dir)s/wget.log"),
          "--lua-script", "puush.lua",
          "--no-check-certificate",
          "--output-document", ItemInterpolation("%(item_dir)s/wget.tmp"),
          "--truncate-output",
          "-e", "robots=off",
          "--rotate-dns",
          "--timeout", "60",
          "--tries", "20",
          "--waitretry", "5",
          "--warc-file", ItemInterpolation("%(item_dir)s/%(warc_file_base)s"),
          "--warc-header", "operator: Archive Team",
          "--warc-header", "puush-dld-script-version: " + VERSION,
          ItemInterpolation("http://puu.sh/%(item_name)s")
        ],
        max_tries=2,
        accept_on_exit_code=[0, EXIT_STATUS_PERMISSION_DENIED,
            EXIT_STATUS_NOT_FOUND],  # see the lua script
    ),
    ConditionalTask(
        is_wget_exit_ok,
        PrepareStatsForTracker(
            defaults={ "downloader": downloader, "version": VERSION },
            file_groups={
                "data": [ ItemInterpolation("%(item_dir)s/%(warc_file_base)s.warc.gz") ]
            },
            id_function=prepare_stats_id_function,
        )
    ),
    ConditionalTask(
        lambda item: not is_wget_exit_ok(item),
        PrepareStatsForTracker(
            defaults={ "downloader": downloader, "version": VERSION },
            file_groups={
                "data": []
            },
            id_function=prepare_stats_id_function,
        )
    ),
    ConditionalTask(is_wget_exit_ok, MoveFiles()),
    CleanUpItemDir(),
    LimitConcurrent(
        NumberConfigValue(min=1, max=4, default="1",
            name="shared:rsync_threads",
            title="Rsync threads",
            description="The maximum number of concurrent uploads."),
        ConditionalTask(
            is_wget_exit_ok,
            UploadWithTracker(
                "http://%s/%s" % (TRACKER_HOST, TRACKER_ID),
                downloader=downloader,
                version=VERSION,
                files=[
                    ItemInterpolation("%(data_dir)s/%(warc_file_base)s.warc.gz")
                ],
                rsync_target_source_path=ItemInterpolation("%(data_dir)s/"),
                rsync_extra_args=[
                "--recursive",
                "--partial",
                "--partial-dir", ".rsync-tmp"
                ]
            )
        )
    ),
    SendDoneToTracker(
        tracker_url="http://%s/%s" % (TRACKER_HOST, TRACKER_ID),
        stats=ItemValue("stats")
    )
)
