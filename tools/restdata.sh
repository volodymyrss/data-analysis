#!/bin/bash

spooldir=/home/savchenk/spool

for i in `seq 4400 4500`; do
    if ! [ -s $spooldir/$i ]; then
        echo "claiming $i"
        hostname > $spooldir/$i
        echo $0 >> $spooldir/$i
        date  >> $spooldir/$i
        break
    fi
done

ssh -Rlocalhost:${i}:localhost:${i} integral@134.158.75.161 "sleep 3600"&

python restdata.py $i
