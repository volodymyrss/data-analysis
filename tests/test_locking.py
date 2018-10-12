import pytest

import glob
import json
import os
import random
import subprocess
import sys

import yaml


package_root=os.path.dirname(os.path.dirname(__file__))

sys.path.insert(0,package_root)
sys.path.insert(0,package_root+"/tests")

import dataanalysis.core as da

rundda_path=package_root+"/tools/rundda.py"

env = os.environ
env['PYTHONPATH'] = package_root + "/tests:" + env.get('PYTHONPATH','')

@pytest.mark.skip(reason="this hangs in travis")
def test_delegation():
    from dataanalysis.caches.queue import QueueCacheWorker
    queue_dir="/tmp/queue"

    print("cache worker...")
    qw=QueueCacheWorker(queue_dir)
    print("cache worker:",qw)

    randomized_version="v%i"%random.randint(1,10000)
    callback_file = "./callback"

    cmd=[
        'python',rundda_path,
        'Mosaic',
        '-m','ddmoduletest',
        '-a','ddmoduletest.RandomModifier(use_version="%s")'%randomized_version,
        '-Q',queue_dir,
        '--callback','file://'+callback_file,
    ]

    if os.path.exists(callback_file):
        os.remove(callback_file)

    exception_report="exception.yaml"
    if os.path.exists(exception_report):
        os.remove(exception_report)

    assert not os.path.exists(callback_file)

    qw.queue.wipe(["waiting","locked","done","failed","running"])

    # run it
    print("CMD:",cmd)
    p=subprocess.Popen(cmd+['--delegate-target'],stdout=subprocess.PIPE,stderr=subprocess.STDOUT,env=env)
    p.wait()
    print(p.stdout.read())

    assert os.path.exists(exception_report)
    recovered_exception = yaml.load(open(exception_report))

    print(recovered_exception)


    #jobs=(glob.glob(queue_dir+"/waiting/*"))
    #assert len(jobs)==1

    #job=yaml.load(open(jobs[0]))

    #print("\n\nJOB",job)


    print(qw.queue.info)

    assert qw.queue.info['waiting'] == 1
    assert qw.queue.info['locked'] == 0
    assert qw.queue.info['done'] == 0

    print("\n\nWORKER")
    qw.run_once()

    print(qw.queue.info)
    assert qw.queue.info['waiting'] == 2
    assert qw.queue.info['locked'] == 1
    assert qw.queue.info['done'] == 0

    assert os.path.exists(callback_file)
    callback_info = open(callback_file).readlines()
    print("".join(callback_info))
    assert len(callback_info) == 1


    print("\n\nWORKER")
    qw.run_once()

    assert qw.queue.info['waiting'] == 1
    assert qw.queue.info['locked'] == 1
    assert qw.queue.info['done'] == 1

    print("\n\nWORKER")
    qw.run_once()

    assert qw.queue.info['waiting'] == 0
    assert qw.queue.info['locked'] == 1
    assert qw.queue.info['done'] == 2


    print("\n\nWORKER run to unlock")
    qw.run_once()

    print(qw.queue.info['waiting'])
    assert qw.queue.info['waiting'] == 0
    assert qw.queue.info['locked'] == 0
    assert qw.queue.info['done'] == 3

    # run again, expecting from cache
    exception_report = "exception.yaml"
    if os.path.exists(exception_report):
        os.remove(exception_report)

    print("\n\nAGAIN")
    print("cmd:"," ".join(cmd+["-V",'--delegate-target']))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,env=env)
    p.wait()
    print(p.stdout.read())

    assert not os.path.exists(exception_report)

    import ddmoduletest

    da.debug_output()

    da.AnalysisFactory.WhatIfCopy("test",ddmoduletest.RandomModifier(use_version=randomized_version))
    A=ddmoduletest.Mosaic()
    A.write_caches=[]
    A.produce_disabled=True
    A.run_if_haveto=False
    A.get()
    assert A._da_output_origin=="cache"
    print(A.resource_stats)
