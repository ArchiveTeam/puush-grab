puush-grab
==========

Grabbing Puu.sh files before they expire.

Please read the wiki at http://archiveteam.org/index.php?title=Puu.sh for the latest information.

Running without a warrior
-------------------------

TODO


Technical details for tracker admin and developers
--------------------------------------------------

When "item name" is referred, they are the base 62 encoded ids of each item. "Item number" refers to the base 10 integer of the item id. The tracker is loaded up by item names (not integers).

The base 62 encoding assumes an alphabet of `0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz` starting from the base 10 integer of 0 and incrementing up. We're not sure if this encoding scheme matches what the website uses internally, but as long as the same alphabet ordering is used by us, it should be ok.


Decentralized Puush Grab Script
-------------------------------

This script will randomly choose Puush items and download them. It does not use a tracker and does not upload files.

This script is useful if you cannot use the Warrior or the tracker is not available. Before running this script, be sure you are willing to invest time running and taking care of the downloads.

The script requires Python 2.7 and wget-lua and can be run with the command as shown:

    python ./decentralized_puush_grab.py

It will create a temporary wget directory, a data directory, and a report directory.

You can provide the `--delay SECONDS` argument to control the minimum delay in seconds.

To stop, create a file called STOP in the same directory.
