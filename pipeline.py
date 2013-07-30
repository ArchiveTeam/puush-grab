from distutils.version import StrictVersion
from seesaw.config import realize, NumberConfigValue
from seesaw.externalprocess import WgetDownload
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.task import SimpleTask, LimitConcurrent
from seesaw.tracker import (TrackerRequest, PrepareStatsForTracker,
    UploadWithTracker, SendDoneToTracker)
from seesaw.util import find_executable
from tornado.ioloop import IOLoop, PeriodicCallback
import fcntl
import json
import os
import pty
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


class GetItemFromTracker(TrackerRequest):
    def __init__(self, tracker_url, downloader, version=None):
        TrackerRequest.__init__(self, "GetItemFromTracker", tracker_url, "request", may_be_canceled=True)
        self.downloader = downloader
        self.version = version

    def data(self, item):
        data = {"downloader": realize(self.downloader, item), "api_version": "2"}
        if self.version:
            data["version"] = realize(self.version, item)
        return data

    def process_body(self, body, item):
        data = json.loads(body)
        if "item_name" in data:
            for (k, v) in data.iteritems():
                item[k] = v
            # #print item
            if not "task_urls" in item:
                # If no task_urls in item, we must get them from task_urls_url
                pattern = item["task_urls_pattern"]
                task_urls_data = urllib2.urlopen(item["task_urls_url"]).read()
                item["task_urls"] = list(pattern % (u,) for u in gunzip_string(task_urls_data).rstrip('\n').decode('utf-8').split(u'\n'))

            item.log_output("Received item '%s' from tracker with %d URLs; first URL is %r\n" % (
                item["item_name"], len(item["task_urls"]), item["task_urls"][0]))
            self.complete_item(item)
        else:
            item.log_output("Tracker responded with empty response.\n")
            self.schedule_retry(item)



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
VERSION = "20130729.00"
USER_AGENT = "ArchiveTeam"
TRACKER_ID = 'puush'

###########################################################################
# This section defines project-specific tasks.
#
# Simple tasks (tasks that do not need any concurrency) are based on the
# SimpleTask class and have a process(item) method that is called for
# each item.

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
            self.warc_prefix, item["item_name"], time.strftime("%Y%m%d-%H%M%S"))

        open("%(item_dir)s/%(warc_file_base)s.warc.gz" % item, "w").close()


class MoveFiles(SimpleTask):
    """
      After downloading, this task moves the warc file from the
      item["item_dir"] directory to the item["data_dir"], and removes
      the files in the item["item_dir"] directory.
      """
    def __init__(self):
        SimpleTask.__init__(self, "MoveFiles")

    def process(self, item):
        os.rename("%(item_dir)s/%(warc_file_base)s.warc.gz" % item,
                "%(data_dir)s/%(warc_file_base)s.warc.gz" % item)

        shutil.rmtree("%(item_dir)s" % item)


###########################################################################
# Initialize the project.
#
# This will be shown in the warrior management panel. The logo should not
# be too big. The deadline is optional.
project = Project(
    title="Puush",
  project_html="""
    <img class="project-logo" alt="" src="http://archiveteam.org/images/b/b2/Puush_logo.png" />
    <h2>Puush <span class="links"><a href="http://puush.me/">Website</a> &middot; <a href="http://tracker.archiveteam.org/%s/">Leaderboard</a></span></h2>
    <p><b>Puush</b> adds expiry dates to their files.</p>
  """ % TRACKER_ID
  # , utc_deadline = datetime.datetime(2013,08,01, 00,00,1)
)

pipeline = Pipeline(
  GetItemFromTracker("http://tracker.archiveteam.org/%s" % TRACKER_ID, downloader, VERSION),
  PrepareDirectories(warc_prefix="xanga.com"),
  WgetDownload([ WGET_LUA,
      "-U", USER_AGENT,
      "-nv",
      "-o", ItemInterpolation("%(item_dir)s/wget.log"),
#      "--lua-script", "puush.lua",
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
    accept_on_exit_code=[ 0],
  ),
  PrepareStatsForTracker(
    defaults={ "downloader": downloader, "version": VERSION },
    file_groups={
      "data": [ ItemInterpolation("%(item_dir)s/%(warc_file_base)s.warc.gz") ]
    }
  ),
  MoveFiles(),
  LimitConcurrent(NumberConfigValue(min=1, max=4, default="1", name="shared:rsync_threads", title="Rsync threads", description="The maximum number of concurrent uploads."),
    UploadWithTracker(
      "http://tracker.archiveteam.org/%s" % TRACKER_ID,
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
    ),
  ),
  SendDoneToTracker(
    tracker_url="http://tracker.archiveteam.org/%s" % TRACKER_ID,
    stats=ItemValue("stats")
  )
)
