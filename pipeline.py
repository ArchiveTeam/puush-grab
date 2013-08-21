from distutils.version import StrictVersion
from seesaw.config import realize, NumberConfigValue
from seesaw.externalprocess import WgetDownload
from seesaw.item import ItemInterpolation, ItemValue
from seesaw.pipeline import Pipeline
from seesaw.project import Project
from seesaw.task import SimpleTask, LimitConcurrent, ConditionalTask, Task
from seesaw.tracker import (TrackerRequest, PrepareStatsForTracker,
    UploadWithTracker, SendDoneToTracker, GetItemFromTracker, RsyncUpload,
    CurlUpload)
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
import re

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
VERSION = "20130821.01"
USER_AGENT = "ArchiveTeam"
# TRACKER_ID = 'test1'
# TRACKER_HOST = 'localhost:8030'
TRACKER_ID = 'puush'
# TRACKER_HOST = 'tracker.archiveteam.org'
TRACKER_HOST = 'b07s57le.corenetworks.net:8031'

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


# Be careful! Some implementations have the ordering of upper and lower case
# differently
ALPHABET = '0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz'


# http://stackoverflow.com/a/1119769/1524507
def base62_encode(num, alphabet=ALPHABET):
    """Encode a number in Base X

    `num`: The number to encode
    `alphabet`: The alphabet to use for encoding
    """
    if (num == 0):
        return alphabet[0]
    arr = []
    base = len(alphabet)
    while num:
        rem = num % base
        num = num // base
        arr.append(alphabet[rem])
    arr.reverse()
    return ''.join(arr)


def base62_decode(string, alphabet=ALPHABET):
    """Decode a Base X encoded string into the number

    Arguments:
    - `string`: The encoded string
    - `alphabet`: The alphabet to use for encoding
    """
    base = len(alphabet)
    strlen = len(string)
    num = 0

    idx = 0
    for char in string:
        power = (strlen - (idx + 1))
        num += alphabet.index(char) * (base ** power)
        idx += 1

    return num


class ExtraItemParams(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, 'ExtraItemParams')

    def process(self, item):
        item_name = item["item_name"]

        if ',' in item_name:
            start_name, end_name = item_name.split(',', 1)
        else:
            start_name = item_name
            end_name = item_name

        start_num = base62_decode(start_name)
        end_num = base62_decode(end_name)

        item['sub_items'] = {}

        assert start_num <= end_num

        for i in xrange(start_num, end_num + 1):
            sub_item_name = base62_encode(i)
            item['sub_items'][sub_item_name] = {
                'wget_exit_status': None,
                'warc_file_base': None,
            }

        item['files_to_upload'] = []

        item.log_output('Sub items: %s' % ', '.join(item['sub_items'].keys()))


class PrepareDirectories(SimpleTask):
    """
      A task that creates temporary directories and initializes filenames.

      It initializes these directories, based on the previously set values
      in ExtraItemParams:
        item["item_dir"] = "%{data_dir}/%{item_name}"
        item['sub_items'][sub_item_name]['warc_file_base'] = [
            "%{warc_prefix}-%{sub_item_name}-%{timestamp}"]

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

        for sub_item_name in item['sub_items'].keys():
            warc_file_base = "%s-%s-%s" % (
                self.warc_prefix,
                sub_item_name,
                time.strftime("%Y%m%d-%H%M%S")
            )

            item['sub_items'][sub_item_name]['warc_file_base'] = warc_file_base

            d = dict(
                item_dir=item['item_dir'],
                warc_file_base=warc_file_base
            )
            open("%(item_dir)s/%(warc_file_base)s.warc.gz" % d, "w").close()


class URLsToDownload(object):
    def realize(self, item):
        l = []
        for sub_item_name in item['sub_items'].keys():
            l.append('http://puu.sh/%s' % sub_item_name)

        return l


class WgetDownloadMany(Task):
    '''Takes in urls, runs wget and generates multiple warcs'''
    def __init__(self, args, urls, retry_delay=30, max_tries=1, accept_on_exit_code=[0], retry_on_exit_code=None, env=None, stdin_data_function=None):
        Task.__init__(self, "WgetDownloadMany")
        self.args = args
        self.max_tries = max_tries
        self.accept_on_exit_code = accept_on_exit_code
        self.retry_on_exit_code = retry_on_exit_code
        self.env = env
        self.stdin_data_function = stdin_data_function
        self.unrealized_urls = urls
        self.retry_delay = retry_delay

    def enqueue(self, item):
        self.start_item(item)
        item.log_output("Starting %s for %s\n" % (self, item.description()))
        item["tries"] = 1
        item['WgetDownloadMany.urls'] = realize(self.unrealized_urls, item)
        item['WgetDownloadMany.urls_index'] = 0
        item['WgetDownloadMany.current_url'] = None
        self.process(item)

    def process(self, item):
        self.set_next_url(item)
        self.process_one(item)

    def set_next_url(self, item):
        urls_index = item['WgetDownloadMany.urls_index']
        urls = item['WgetDownloadMany.urls']

        if urls_index < len(urls):
            item['WgetDownloadMany.current_url'] = urls[urls_index]
            item['WgetDownloadMany.urls_index'] += 1
            return True
        else:
            return False

    def process_one(self, item):
        with self.task_cwd():
            url = item['WgetDownloadMany.current_url']

            item.log_output("Start downloading URL %s" % url)

            p = seesaw.externalprocess.AsyncPopen(
              args=realize(self.args, item) + [url],
              env=realize(self.env, item),
              stdin=subprocess.PIPE,
              close_fds=True
            )

            p.on_output += functools.partial(self.on_subprocess_stdout, p, item)
            p.on_end += functools.partial(self.on_subprocess_end, item)

            p.run()

            p.stdin.write(self.stdin_data(item))
            p.stdin.close()

    def stdin_data(self, item):
        if self.stdin_data_function:
            return self.stdin_data_function(item)
        else:
            return ""

    def on_subprocess_stdout(self, pipe, item, data):
        item.log_output(data, full_line=False)

    def on_subprocess_end(self, item, returncode):
        if returncode in self.accept_on_exit_code:
            self.handle_process_result(returncode, item)
        else:
            self.handle_process_error(returncode, item)

    def handle_process_result(self, exit_code, item):
        if self.set_next_url(item):
            self.process_one(item)
        else:
            item.log_output("Finished %s for %s\n" % (self, item.description()))
            self.complete_item(item)

    def handle_process_error(self, exit_code, item):
        item["tries"] += 1

        item.log_output("Process %s returned exit code %d for %s\n" % (self, exit_code, item.description()))
        item.log_error(self, exit_code)

        if (self.max_tries == None or item["tries"] < self.max_tries) and (self.retry_on_exit_code == None or exit_code in self.retry_on_exit_code):
            item.log_output("Retrying %s for %s after %d seconds...\n" % (self, item.description(), self.retry_delay))
            IOLoop.instance().add_timeout(datetime.timedelta(seconds=self.retry_delay),
                functools.partial(self.process_one, item))

        else:
            item.log_output("Failed %s for %s\n" % (self, item.description()))
            self.fail_item(item)


class SpecializedWgetDownloadMany(WgetDownloadMany):
    SUCCESS_DELAY = 0.2  # seconds
    MAX_ERROR_DELAY = 60 * 5  # seconds
    MIN_ERROR_DELAY = 10.0  # seconds
    EXP_RATE = 1.5
    current_error_delay = MIN_ERROR_DELAY  # seconds

    def process_one(self, item):
        sub_item_name = item['WgetDownloadMany.current_url'].rsplit('/', 1)[-1]

        item['current_warc_file_base'] = item['sub_items'][sub_item_name
            ]['warc_file_base']

        WgetDownloadMany.process_one(self, item)

    def save_exit_code(self, exit_code, item):
        sub_item_name = item['WgetDownloadMany.current_url'].rsplit('/', 1)[-1]
        item['sub_items'][sub_item_name]['wget_exit_status'] = exit_code

    def handle_process_result(self, exit_code, item):
        self.save_exit_code(exit_code, item)
        delay_seconds = random.uniform(self.SUCCESS_DELAY * 0.5,
                self.SUCCESS_DELAY * 2.0)
        self.current_error_delay = self.MIN_ERROR_DELAY

        IOLoop.instance().add_timeout(
            datetime.timedelta(seconds=delay_seconds),
            functools.partial(WgetDownloadMany.handle_process_result,
                self, exit_code, item))

    def handle_process_error(self, exit_code, item):
        self.save_exit_code(exit_code, item)

        if exit_code == EXIT_STATUS_OTHER_ERROR:
            self.current_error_delay *= self.EXP_RATE
            self.current_error_delay = min(self.current_error_delay,
                self.MAX_ERROR_DELAY)

            delay_seconds = self.current_error_delay
            item.log_output('Unexpected response from server. '
                'Waiting for %d seconds before continuing...' % delay_seconds)
            self.retry_delay = 0  # we'll use our own delay
            IOLoop.instance().add_timeout(
                datetime.timedelta(seconds=delay_seconds),
                functools.partial(WgetDownloadMany.handle_process_error,
                    self, exit_code, item))
        else:
            self.retry_delay = 30
            WgetDownloadMany.handle_process_error(self, exit_code, item)


class MoveFiles(SimpleTask):
    """
      After downloading, this task moves the warc files from the
      item["item_dir"] directory to the item["data_dir"]
      """
    def __init__(self):
        SimpleTask.__init__(self, "MoveFiles")

    def process(self, item):
        for sub_item_name in item['sub_items'].keys():
            wget_exit_status = item['sub_items'][sub_item_name][
                'wget_exit_status']
            warc_file_base = item['sub_items'][sub_item_name][
                'warc_file_base']

            # Reject 404s and permission denieds
            if wget_exit_status != 0:
                continue

            d = dict(
                item_dir=item['item_dir'],
                data_dir=item['data_dir'],
                warc_file_base=warc_file_base,
            )

            os.rename("%(item_dir)s/%(warc_file_base)s.warc.gz" % d,
                "%(data_dir)s/%(warc_file_base)s.warc.gz" % d)

            item['files_to_upload'].append(
                "%(data_dir)s/%(warc_file_base)s.warc.gz" % d)


class PrepareStatsForTracker2(SimpleTask):
    '''Similar to PrepareStatsForTracker but calls realize on files earlier'''
    def __init__(self, defaults=None, file_groups=None, id_function=None):
        SimpleTask.__init__(self, "PrepareStatsForTracker2")
        self.defaults = defaults or {}
        self.file_groups = file_groups or {}
        self.id_function = id_function

    def process(self, item):
        total_bytes = {}
        for (group, files) in self.file_groups.iteritems():
            total_bytes[group] = sum([ os.path.getsize(f) for f in realize(files, item)])

        stats = {}
        stats.update(self.defaults)
        stats["item"] = item["item_name"]
        stats["bytes"] = total_bytes

        if self.id_function:
            stats["id"] = self.id_function(item)

        item["stats"] = realize(stats, item)


class CleanUpItemDir(SimpleTask):
    def __init__(self):
        SimpleTask.__init__(self, "CleanUpItemDir")

    def process(self, item):
        shutil.rmtree("%(item_dir)s" % item)


class FilesToUpload(object):
    def realize(self, item):
        return files_to_upload(item)


def files_to_upload(item):
    return item['files_to_upload']


def prepare_stats_id_function(item):
    d = {'wget_exit_statuses': {}}

    for sub_item_name in item['sub_items'].keys():
        d['wget_exit_statuses'][sub_item_name] = item['sub_items'][
            sub_item_name]['wget_exit_status']

    return json.dumps(d)


class UploadWithTracker2(TrackerRequest):
    '''Similar to UploadWithTracker but calls realize on files earlier'''
    def __init__(self, tracker_url, downloader, files, version=None, rsync_target_source_path="./", rsync_bwlimit="0", rsync_extra_args=[], curl_connect_timeout="60", curl_speed_limit="1", curl_speed_time="900"):
        TrackerRequest.__init__(self, "Upload2", tracker_url, "upload")

        self.downloader = downloader
        self.version = version

        self.files = files
        self.rsync_target_source_path = rsync_target_source_path
        self.rsync_bwlimit = rsync_bwlimit
        self.rsync_extra_args = rsync_extra_args
        self.curl_connect_timeout = curl_connect_timeout
        self.curl_speed_limit = curl_speed_limit
        self.curl_speed_time = curl_speed_time

    def data(self, item):
        data = {"downloader": realize(self.downloader, item),
                "item_name": item["item_name"]}
        if self.version:
            data["version"] = realize(self.version, item)
        return data

    def process_body(self, body, item):
        data = json.loads(body)
        if "upload_target" in data:
            files = realize(self.files, item)
            inner_task = None

            if re.match(r"^rsync://", data["upload_target"]):
                item.log_output("Uploading with Rsync to %s" % data["upload_target"])
                inner_task = RsyncUpload(data["upload_target"], files, target_source_path=self.rsync_target_source_path, bwlimit=self.rsync_bwlimit, extra_args=self.rsync_extra_args, max_tries=1)

            elif re.match(r"^https?://", data["upload_target"]):
                item.log_output("Uploading with Curl to %s" % data["upload_target"])

                if len(files) != 1:
                    item.log_output("Curl expects to upload a single file.")
                    self.fail_item(item)
                    return

                inner_task = CurlUpload(data["upload_target"], files[0], self.curl_connect_timeout, self.curl_speed_limit, self.curl_speed_time, max_tries=1)

            else:
                item.log_output("Received invalid upload type.")
                self.fail_item(item)
                return

            inner_task.on_complete_item += self._inner_task_complete_item
            inner_task.on_fail_item += self._inner_task_fail_item
            inner_task.enqueue(item)

        else:
            item.log_output("Tracker did not provide an upload target.")
            self.schedule_retry(item)

    def _inner_task_complete_item(self, task, item):
        self.complete_item(item)

    def _inner_task_fail_item(self, task, item):
        self.schedule_retry(item)



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
    SpecializedWgetDownloadMany([ WGET_LUA,
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
          "--warc-file", ItemInterpolation("%(item_dir)s/%(current_warc_file_base)s"),
          "--warc-header", "operator: Archive Team",
          "--warc-header", "puush-dld-script-version: " + VERSION,
        ],
        URLsToDownload(),
        max_tries=20,
        accept_on_exit_code=[
            0,
            EXIT_STATUS_PERMISSION_DENIED,
            EXIT_STATUS_NOT_FOUND
        ],  # see the lua script, also MoveFiles
    ),
    MoveFiles(),
    PrepareStatsForTracker2(
        defaults={ "downloader": downloader, "version": VERSION },
        file_groups={
            "data": FilesToUpload(),
        },
        id_function=prepare_stats_id_function,
    ),
    CleanUpItemDir(),
    LimitConcurrent(
        NumberConfigValue(min=1, max=4, default="1",
            name="shared:rsync_threads",
            title="Rsync threads",
            description="The maximum number of concurrent uploads."),
        ConditionalTask(
            files_to_upload,
            UploadWithTracker2(
                "http://%s/%s" % (TRACKER_HOST, TRACKER_ID),
                downloader=downloader,
                version=VERSION,
                files=FilesToUpload(),
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
