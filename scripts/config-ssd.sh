#!/bin/bash

#if [ $EUID -ne 0 ]; then
#   echo "This script must be run as root" 
#   exit 1
#fi

#mkfs.ext4 /dev/nvme1n1p1
mkdir -p /scratch0
#umount /dev/nvme0n1p1 
#mount -o rw,noatime,nodiratime,block_validity,delalloc,nojournal_checksum,barrier,user_xattr,acl /dev/nvme0n1p1 /scratch0/
mkdir -p /scratch0/tenvisdb
#chown abc -R /scratch0/tenvisdb
#chgrp abc-R /scratch0/tenvisdb

ulimit -n 4096
