puush-grab
==========

Grabbing Puu.sh files before they expire.

Please read the wiki at http://archiveteam.org/index.php?title=Puu.sh for the lastest information.

Running without a warrior
-------------------------

TODO


Decentralized Puush Grab Script
===============================

This script will randomly choose Puush items and download them. It does not use a tracker and does not upload files.

This script is useful if you cannot use the Warrior or the tracker is not available. Before running this script, be sure you are willing to invest time running and taking care of the downloads.

The script requires Python 2.7 and wget-lua and can be run with the command as shown:

    python ./decentralized_puush_grab.py

It will create a temporary wget directory, a data directory, and a report directory.

You can provide the `--delay SECONDS` argument to control the minimum delay in seconds.

To stop, create a file called STOP in the same directory.
