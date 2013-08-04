puush-grab
==========

Grabbing Puu.sh files before they expire.

Please read the wiki at http://archiveteam.org/index.php?title=Puu.sh for the latest information.

Running without a warrior
-------------------------

To run this outside the warrior:

(Ubuntu / Debian 7)

    sudo apt-get update
    sudo apt-get install -y build-essential lua5.1 liblua5.1-0-dev python python-setuptools python-dev git-core openssl libssl-dev python-pip rsync gcc make git screen
    pip install --user seesaw
    git clone https://github.com/ArchiveTeam/puush-grab
    cd puush-grab
    ./get-wget-lua.sh
    
    # Start downloading with:
    screen ~/.local/bin/run-pipeline --disable-web-server pipeline.py YOURNICKNAME

(Debian 6)

    sudo apt-get update
    sudo apt-get install -y build-essential lua5.1 liblua5.1-0-dev python python-setuptools python-dev git-core openssl libssl-dev python-pip rsync gcc make git screen
    wget --no-check-certificate https://pypi.python.org/packages/source/p/pip/pip-1.3.1.tar.gz
    tar -xzvf pip-1.3.1.tar.gz
    cd pip-1.3.1
    python setup.py install --user
    cd ..
    ~/.local/bin/pip install --user seesaw
    git clone https://github.com/ArchiveTeam/puush-grab
    cd puush-grab
    ./get-wget-lua.sh

    # Start downloading with:
    screen ~/.local/bin/run-pipeline --disable-web-server pipeline.py YOURNICKNAME

(CentOS / RHEL / Amazon Linux)

    sudo yum install lua lua-devel python-devel python-distribute git openssl-devel rsync gcc make screen
    wget --no-check-certificate https://pypi.python.org/packages/source/p/pip/pip-1.3.1.tar.gz
    tar -xzvf pip-1.3.1.tar.gz
    cd pip-1.3.1
    python setup.py install --user
    cd ..
    ~/.local/bin/pip install --user seesaw
    git clone https://github.com/ArchiveTeam/puush-grab
    cd puush-grab
    ./get-wget-lua.sh

    # Start downloading with:
    screen ~/.local/bin/run-pipeline --disable-web-server pipeline.py YOURNICKNAME

For more options, run:

    ~/.local/bin/run-pipeline --help


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
