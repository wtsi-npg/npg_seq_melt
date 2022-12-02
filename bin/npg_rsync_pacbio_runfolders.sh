#!/bin/bash
#
# Copyright (C) 2022 Genome Research Ltd. All rights reserved.
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.


set -euo pipefail

usage() {
    cat 1>&2 << EOF

This script mirrors the contents of runfolders in the staging directory
to a backup directory using rsync. It is based on rsync_runfolders.sh
(author: Keith James). 

It is run periodically by a cron job and will copy each runfolder it
encounters to a directory with the same name in the backup directory.

Example:

  /source/r64229e_20221125_113918

  will be copied to:

  /destination/r64229e_20221125_113918

The destination server is chosen automatically by the accompanying
rsync_destination.sh script (which must be present in the same
directory as this script).

This script will copy only files identified as having originated on
the sequencing instrument: selection is by the user id used for this
instrument data staging purpose.

This script can start as many processes as there are runfolders when
it is run and it will wait until they have completed before exiting.

While running, this script will acquire locks on a file for each
runfolder, named \$LOCK_DIR/<runfolder name>.rsync.lock. If it cannot
acquire a lock, it will log the attempt and the subprocess will
exit. Other processes may test the lock in order to determine whether
there is an rsync operation in progress.

Example:

  rsync_pacbio_runfolders.sh \\
    -d /pacbio/disaster_recovery \\
       /pacbio/staging/*

This script logs important events to the syslog.

Assuming a runfolder size of 1.2 Tb and a desired transfer time of
<24 hours, the bandwidth limit should be set to

1000^4 / 24 x 60 x 60 B/s = 11,574,074 B/s

i.e. 12,000 kB/s

Usage: $0 -d <rsync destination>
  [-l mtime ]
  [-m daemon | rsh]
  [-n <nice level>]
  [-o <username>]
  [-b <bandwidth limit>] [-h] [-v] <runfolders>

Options:

 -b  Bandwidth limit for rsync, KB/s. Optional, defaults to 12,000 kB/s.

 -d  An rsync destination path. Required.

 -h  Print usage and exit.

 -l  Last modified. Only copy files modified in the last l days. This
     is implemented using find -mtime. See the find manpage for the effects
     of rounding on this value. Optional, defaults to -7 (within the last 7
     days).

 -n  A "nice" priority level for the rsync process. Optional, defaults
     to 19.

 -o  Owner (username) of the files to copy. Optional, defaults to pb.

 -s  Sequential mode. Instead of launching all rsync jobs in parallel
     (the default), run them sequentially.

 -v  Print verbose messages.
EOF
}

get_current_time() {
    awk 'BEGIN { print systime() }'
}

print_iso8601_time() {
    local timestamp="$1"
    echo "$timestamp" | awk '{ print strftime("%FT%T%z", $1) }'
}

log_notice() {
    local msg="$1"
    logger -sp user.notice -t $LOG_TAG "$msg"
}

log_warn() {
    local msg="$1"
    logger -sp user.warn -t $LOG_TAG "$msg"
}

log_error() {
    local msg="$1"
    logger -sp user.err -t $LOG_TAG "$msg"
}

SCRIPT_PATH=$(readlink -f $BASH_SOURCE)
SCRIPT_DIR=$(dirname $SCRIPT_PATH)

LOG_TAG=rsync_runfolders

RSYNC=rsync
RSYNC_NICE=19
RSYNC_SEQUENTIAL=
LAST_MODIFIED="-7"
FILE_OWNER=pb

BANDWIDTH_LIMIT=12000 # kB/s
LOCK_DIR=/var/lock

DEST=

while getopts "b:d:hl:n:o:p:sv" option; do
    case "$option" in
        b)
            BANDWIDTH_LIMIT="$OPTARG"
            ;;
        d)
            DEST="$OPTARG"
            ;;
        h)
            usage
            exit 0
            ;;
        l)
            LAST_MODIFIED="$OPTARG"
            ;;
        m)
            RSYNC_MODE="$OPTARG"
            ;;
        n)
            RSYNC_NICE="$OPTARG"
            ;;
        o)
            FILE_OWNER="$OPTARG"
            ;;
        s)
            RSYNC_SEQUENTIAL=1
            ;;
        v)
            set -x
            ;;
        *)
            usage
            echo "Invalid argument!"
            exit 4
            ;;
    esac
done

shift $((OPTIND -1))

if [ -z "$DEST" ] ; then
    usage
    echo -e "\nERROR:\n  A -d <rsync destination> argument is required"
    exit 4
fi

if [ "$RSYNC_NICE" -ne "$RSYNC_NICE" ]; then
    usage
    echo -e "\nERROR:\n  The -n <nice priority> value must be an integer"
fi


# Associative array of PID to runfolder path
declare -A PID_RUNFOLDERS
NUM_ERRORS=0

for runfolder in "$@" ; do
    if [ -d "$runfolder" ]; then
        # Remove any redundant slashes in input e.g. /a//b
        source_path=$(echo "$runfolder" | sed -e 's_\/\{2,\}_\/_g')
        dest_path=$(echo "$DEST" | sed -e 's_\/\{2,\}_\/_g')

        # Remove any trailing slash in input
        source_path=$(echo "$source_path" | sed -e 's_\/$__')
        dest_path=$(echo "$dest_path" | sed -e 's_\/$__')

        runfolder_name=$(basename "$source_path")

        # Lock file to ensure only one rsync process runs on a
        # runfolder
        runfolder_lockfile="$LOCK_DIR/$runfolder_name.rsync.lock"
        touch "$runfolder_lockfile"

        timestamp=$(print_iso8601_time $(get_current_time))
        pid="$BASHPID"

        unset lockfd
        exec {lockfd}< "$runfolder_lockfile"
        flock -nx $lockfd ||
            {
                log_warn \
                    "process $pid could not get lock on $runfolder_lockfile"
                continue
            }

        log_notice "process $pid obtained a lock on $runfolder_lockfile"
        log_notice "rsyncing files from $source_path to $dest_path"

        runfolder_parent=$(dirname $source_path)
        
        # Not using --archive because that implies copying symlinks,
        # character and block device files, and special files such as
        # named sockets and fifos
        nice -n $RSYNC_NICE $RSYNC "$runfolder_parent" "$dest_path" \
             --files-from=<(cd "$runfolder_parent" && find "$runfolder_name" -user "$FILE_OWNER" -type f -mtime "$LAST_MODIFIED") \
             --perms \
             --times \
             --group \
             --owner \
             --no-dirs \
             --prune-empty-dirs \
             --bwlimit="$BANDWIDTH_LIMIT" \
             2> >(logger -sp user.err -t $LOG_TAG) & pid=$!

        # Store mapping of PID to runfolder path
        PID_RUNFOLDERS[$pid]="$source_path"

        if [ -n "$RSYNC_SEQUENTIAL" ]; then
            folder="${PID_RUNFOLDERS[$pid]}"
            log_notice "sequential mode: waiting for rsync process $pid copying $folder"

            if ! wait $pid; then
                (( NUM_ERRORS += 1 ))
                log_error "rsync process $pid copying $folder failed"
            else
                log_notice "rsync process $pid copying $folder succeeded"
            fi
        fi
    fi
done

num_procs="${#PID_RUNFOLDERS[@]}"

if [ -z "$RSYNC_SEQUENTIAL" ]; then
    if [ "$num_procs" -gt 0 ] ; then
        for pid in "${!PID_RUNFOLDERS[@]}" ; do
            folder="${PID_RUNFOLDERS[$pid]}"
            if ! wait $pid; then
                (( NUM_ERRORS += 1 ))
                log_error "rsync process $pid copying $folder failed"
            else
                log_notice "rsync process $pid copying $folder succeeded"
            fi
        done
    fi
fi

if [ $NUM_ERRORS -ne 0 ]; then
    log_error "$num_procs rsync processes done with $NUM_ERRORS errors"
    exit 3
else
    log_notice "$num_procs rsync processes done with $NUM_ERRORS errors"
fi
