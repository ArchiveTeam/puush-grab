puush-grab
==========

Grabbing Puu.sh files before they expire.

Please read the wiki at http://archiveteam.org/index.php?title=Puu.sh for the latest information.

Setup instructions
=========================

Be sure to replace `YOURNICKHERE` with the nickname that you want to be shown as, on the tracker. You don't need to register it, just pick a nickname you like.

In most of the below cases, there will be a web interface running at http://localhost:8001/. If you don't know or care what this is, you can just ignore it—otherwise, it gives you a fancy view of what's going on.

**If anything goes wrong while running the commands below, please scroll down to the bottom of this page. There's troubleshooting information there.**

Running without a warrior
-------------------------
To run this outside the warrior, clone this repository, cd into its directory and run:

    pip install seesaw
    ./get-wget-lua.sh

then start downloading with:

    run-pipeline pipeline.py --concurrent 2 YOURNICKHERE

For more options, run:

    run-pipeline --help

If you don't have root access and/or your version of pip is very old, you can replace "pip install seesaw" with:

    wget https://raw.github.com/pypa/pip/master/contrib/get-pip.py ; python get-pip.py --user ; ~/.local/bin/pip install --user seesaw

so that pip and seesaw are installed in your home, then run

    ~/.local/bin/run-pipeline pipeline.py --concurrent 2 YOURNICKHERE

Distribution-specific setup
-------------------------
### For Debian/Ubuntu:

    adduser --system --group --shell /bin/bash archiveteam
    apt-get install -y git-core libgnutls-dev lua5.1 liblua5.1-0 liblua5.1-0-dev screen python-dev python-pip bzip2 zlib1g-dev
    pip install seesaw
    su -c "cd /home/archiveteam; git clone https://github.com/ArchiveTeam/puush-grab.git; cd puush-grab; ./get-wget-lua.sh" archiveteam
    screen su -c "cd /home/archiveteam/{{REPO_NAME}}/; run-pipeline pipeline.py --concurrent 2 --address '127.0.0.1' YOURNICKHERE" archiveteam
    [... ctrl+A D to detach ...]

Wget-lua is also available on [ArchiveTeam's PPA](https://launchpad.net/~archiveteam/+archive/wget-lua) for Ubuntu.

### For CentOS:

Ensure that you have the CentOS equivalent of bzip2 installed as well. You might need the EPEL repository to be enabled.

    yum -y install gnutls-devel lua-devel python-pip zlib-devel
    pip install seesaw
    [... pretty much the same as above ...]

### For openSUSE:

    zypper install liblua5_1 lua51 lua51-devel screen python-pip libgnutls-devel bzip2 python-devel gcc make
    pip install seesaw
    [... pretty much the same as above ...]

### For OS X:

You need Homebrew. Ensure that you have the OS X equivalent of bzip2 installed as well.

    brew install python lua gnutls
    pip install seesaw
    [... pretty much the same as above ...]

**There is a known issue with some packaged versions of rsync. If you get errors during the upload stage, {{REPO_NAME}} will not work with your rsync version.**

This supposedly fixes it:

    alias rsync=/usr/local/bin/rsync

### For Arch Linux:

Ensure that you have the Arch equivalent of bzip2 installed as well.

1. Make sure you have `python-pip2` installed.
2. Install [https://aur.archlinux.org/packages/wget-lua/](the wget-lua package from the AUR). 
3. Run `pip2 install seesaw`.
4. Modify the run-pipeline script in seesaw to point at `#!/usr/bin/python2` instead of `#!/usr/bin/python`.
5. `adduser --system --group --shell /bin/bash archiveteam`
6. `screen su -c "cd /home/archiveteam/puush-grab/; run-pipeline pipeline.py --concurrent 2 --address '127.0.0.1' YOURNICKHERE" archiveteam`

### For FreeBSD:

Honestly, I have no idea. `./get-wget-lua.sh` supposedly doesn't work due to differences in the `tar` that ships with FreeBSD. Another problem is the apparent absence of Lua 5.1 development headers. If you figure this out, please do let us know on IRC (irc.efnet.org #archiveteam).

Troubleshooting
=========================

Broken? These are some of the possible solutions:

### wget-lua was not successfully built

If you get errors about `wget.pod` or something similar, the documentation failed to compile - wget-lua, however, compiled fine. Try this:

    cd get-wget-lua.tmp
    mv src/wget ../wget-lua
    cd ..

The `get-wget-lua.tmp` name may be inaccurate. If you have a folder with a similar but different name, use that instead and please let us know on IRC what folder name you had!

Optionally, if you know what you're doing, you may want to use wgetpod.patch.

### Problem with gnutls or openssl during get-wget-lua

Please ensure that gnutls-dev(el) and openssl-dev(el) are installed.

### ImportError: No module named seesaw

If you're sure that you followed the steps to install `seesaw`, permissions on your module directory may be set incorrectly. Try the following:

    chmod o+rX -R /usr/local/lib/python2.7/dist-packages

### Issues in the code

If you notice a bug and want to file a bug report, please use the GitHub issues tracker.

Are you a developer? Help write code for us! Look at our [developer documentation](http://archiveteam.org/index.php?title=Dev) for details.

### Other problems

Have an issue not listed here? Join us on IRC and ask! We can be found at irc.efnet.org #pushharder.

Technical details for tracker admin and developers
==================================================

When "item name" is referred, they are the base 62 encoded ids of each item. "Item number" refers to the base 10 integer of the item id. The tracker is loaded up by item names (not integers).

The script uses two base 62 encoding alphabets. The legacy alphabet consists of `0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz`. Puush, however, uses an encoding of `0123456789abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ`. They both start from the base 10 integer of 0 and incrementing up.  Therefore, it is important to be careful of this distinction. 

(When transitioning from one alphabet and generating item names, be sure to use a value that is unambiguously lower than the current item number in *both* alphabets. Then, switch alphabets.)

If the item contains a comma like ``abcd,abcg``, it is an item name range using the legacy alphabet. It covers from ``abcd`` to ``abcg`` which is 4 items in the range. If the item contains a colon like `abcd:abcg`, it is an item name using the Puush alphabet.


Decentralized Puush Grab Script
-------------------------------

This script will randomly choose Puush items and download them. It does not use a tracker and does not upload files.

This script is useful if you cannot use the Warrior or the tracker is not available. Before running this script, be sure you are willing to invest time running and taking care of the downloads.

The script requires Python 2.7 and wget-lua and can be run with the command as shown:

    python ./decentralized_puush_grab.py

It will create a temporary wget directory, a data directory, and a report directory.

You can provide the `--delay SECONDS` argument to control the minimum delay in seconds.

To stop, create a file called STOP in the same directory.

