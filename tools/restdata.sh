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

trap "{ echo cleaning up; rm -fv $spooldir/$i ; exit 255; }" EXIT SIGINT SIGTERM

ssh -Rlocalhost:${i}:localhost:${i} integral@134.158.75.161 "sleep 100000"&

export TMPDIR=$PWD
python /Integral/throng/common/soft/dda/core/tools/restdata.py $i > $spooldir/logs/workers/fulllog_${i}_${HOSTNAME} 2>&1
#python /Integral/throng/common/soft/dda/core/tools/restdata.py $i #> $spooldir/logs/workers/fulllog_${i}_${HOSTNAME}


#python /Integral/throng/common/soft/dda/core/tools/restdata.py $i > $spooldir/logs/jobs/${i}_${HOSTNAME}_`date +%s`
