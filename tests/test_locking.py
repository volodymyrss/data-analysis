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

rundda_path="-m dataanalysis.rundda"

env = os.environ
env['PYTHONPATH'] = package_root + "/tests:" + env.get('PYTHONPATH','')

@pytest.mark.mysql
def test_delegation():
    from dataanalysis.caches.queue import QueueCacheWorker
    queue_name="/tmp/queue"
    qw=QueueCacheWorker(queue_name)
    qw.queue.purge()
    print(("cache worker:",qw))

    randomized_version="v%i"%random.randint(1,10000)
    callback_file = "./callback"

    cmd=[
        'dda-run',
        'Mosaic',
        '-m','ddmoduletest',
        '-a','ddmoduletest.RandomModifier(use_version="%s")'%randomized_version,
#        '-Q',queue_name,
        '--callback','file://'+callback_file,
        '--delegate-target',
    ]


    if os.path.exists(callback_file):
        os.remove(callback_file)

    exception_report="exception.yaml"
    if os.path.exists(exception_report):
        os.remove(exception_report)

    assert not os.path.exists(callback_file)

    qw.queue.wipe(["waiting","locked","done","failed","running"])

    # run it
    print(("CMD:"," ".join(cmd)))

    try:
        subprocess.check_call(cmd,stderr=subprocess.STDOUT,env=env)
    except subprocess.CalledProcessError as e:
        pass
    else:
        raise Exception("expected AnalysisDelegatedException")

    #p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, env=env)
    #p.wait()
    #print(p.stdout.read())

    assert os.path.exists(exception_report)
    recovered_exception = yaml.load(open(exception_report))

    print(recovered_exception)

    print((qw.queue.info))

    assert qw.queue.info['waiting'] == 1, qw.queue.info
    assert qw.queue.info['locked'] == 0
    assert qw.queue.info['done'] == 0

    print("\n\nWORKER")
    qw.run_once()

    print((qw.queue.info))
    assert qw.queue.info['waiting'] == 2
    assert qw.queue.info['locked'] == 1
    assert qw.queue.info['done'] == 0

    assert os.path.exists(callback_file)
    callback_info = open(callback_file).readlines()
    print(("".join(callback_info)))
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


    print("\n\nWORKER run to unlock does not work anymore")
    qw.run_once()
    assert qw.queue.info['waiting'] == 0
    assert qw.queue.info['locked'] == 1
    assert qw.queue.info['done'] == 2

    print("\n\nWORKER run to unlock does not work anymore")
    qw.queue.try_all_locked()
    assert qw.queue.info['waiting'] == 1
    assert qw.queue.info['locked'] == 0
    assert qw.queue.info['done'] == 2

    qw.run_once()
    assert qw.queue.info['waiting'] == 0
    assert qw.queue.info['locked'] == 0
    assert qw.queue.info['done'] == 3

    # run again, expecting from cache
    exception_report = "exception.yaml"
    if os.path.exists(exception_report):
        os.remove(exception_report)

    print("\n\nAGAIN")
    print(("cmd:"," ".join(cmd+["-V",'--delegate-target'])))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,env=env)
    p.wait()
    print((p.stdout.read()))

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
    print((A.resource_stats))
