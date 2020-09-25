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

rundda_command="dda-run"

env = os.environ
env['PYTHONPATH'] = package_root + "/tests:" + env.get('PYTHONPATH','')



def test_simple():
    cmd=[
        rundda_command,
        'ClientDelegatableAnalysisA',
        '-m','ddmoduletest',
        '-a','ddmoduletest.ClientDelegatableAnalysisA(use_sleep=0.2,use_cache=ddmoduletest.server_local_cache)',
        '-f','ClientDelegatableAnalysisA',
    ]
    p=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,env=env)
    p.wait()
    print((p.stdout.read()))

    p = subprocess.Popen(cmd[:-2], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,env=env)
    p.wait()
    print((p.stdout.read()))


@pytest.mark.skip(reason="this hangs sometimes")
def test_prompt_delegation():
    from dataanalysis.caches.queue import QueueCacheWorker
    queue_dir="/tmp/queue"
    qw=QueueCacheWorker(queue_dir)

    randomized_version="v%i"%random.randint(1,10000)

    injection_fn = "injection.json"
    json.dump(['ClientDelegatableAnalysisA',
               {
                   "data_add": "added"
               }
               ],
              open(injection_fn,"w"),
              )
    
    callback_file = os.getcwd()+"/callback"

    cmd=[
        rundda_command,
        'ClientDelegatableAnalysisA',
        '-m','ddmoduletest',
        '-a','ddmoduletest.ClientDelegatableAnalysisA(use_sleep=0.2,use_cache=ddmoduletest.server_local_cache,use_version="%s")'%randomized_version,
        '-i',injection_fn,
        '-V','-x',
        '-D',queue_dir,
        #'--callback', 'file://' + callback_file,
    ]

    qw.queue.purge()

    exception_report="exception.yaml"
    if os.path.exists(exception_report):
        os.remove(exception_report)

    # run it
    p=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,env=env)
    p.wait()
    print((p.stdout.read()))

    assert os.path.exists(exception_report)
    recovered_exception=yaml.load(open(exception_report))

    print(recovered_exception)

    print((qw.queue.info))
    assert qw.queue.info['waiting']==1

    da.debug_output()
    qw.run_once()

    A = da.byname('ClientDelegatableAnalysisA')
    assert A.sleep == 0.2
    assert A.version == randomized_version

    print(("recovered object:", A))


    # run again, expecting from cache
    exception_report = "exception.yaml"
    if os.path.exists(exception_report):
        os.remove(exception_report)

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,env=env)
    p.wait()
    print((p.stdout.read()))

    import ddmoduletest

    da.debug_output()

    A=ddmoduletest.ClientDelegatableAnalysisA(use_sleep=0.2,use_cache=ddmoduletest.server_local_cache,use_version=randomized_version)
    A.write_caches=[]
    A.produce_disabled=True
    A.run_if_haveto=False
    A.get()
    assert A._da_output_origin=="cache"
    assert A.data=="dataAadded"
    print((A.resource_stats))

@pytest.mark.skip(reason="test this later in py3 ") # TODO
def test_delegation():
    from dataanalysis.caches.queue import QueueCacheWorker
    queue_dir="/tmp/queue"
    qw=QueueCacheWorker(queue_dir)
    qw.queue.purge()

    randomized_version="v%i"%random.randint(1,10000)
    callback_file = os.getcwd()+"/callback"




    cmd=[
        rundda_command,
        'ClientDelegatableAnalysisA',
        '-m','ddmoduletest',
        '-a','ddmoduletest.ClientDelegatableAnalysisA(use_sleep=0.2,use_cache=ddmoduletest.server_local_cache,use_version="%s")'%randomized_version,
        '-Q',queue_dir,
        '--callback','file://'+callback_file,
        '--delegate-target',
    ]


    if os.path.exists(callback_file):
        os.remove(callback_file)

    exception_report="exception.yaml"
    if os.path.exists(exception_report):
        os.remove(exception_report)

    assert not os.path.exists(callback_file)

    # run it
    p=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,env=env)
    p.wait()
    print((p.stdout.read()))

    assert os.path.exists(exception_report)
    recovered_exception = yaml.load(open(exception_report))

    print(recovered_exception)


    print((qw.queue.info))
    assert qw.queue.info['waiting']==1

    print("\n\nWORKER")
    qw.run_once()

    assert os.path.exists(callback_file)
    callback_info = open(callback_file).readlines()
    print(("".join(callback_info)))
    assert len(callback_info) == 6


    # run again, expecting from cache
    exception_report = "exception.yaml"
    if os.path.exists(exception_report):
        os.remove(exception_report)

    print("\n\nAGAIN")
    print(("cmd:"," ".join(cmd+["-v", "-d ClientDelegatableAnalysisA"])))
    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,env=env)
    p.wait()
    print((p.stdout.read()))
    
    print(("queue after again:", qw.queue.info))

    assert not os.path.exists(exception_report)

    import ddmoduletest

    da.debug_output()

    A=ddmoduletest.ClientDelegatableAnalysisA(use_sleep=0.2,use_cache=ddmoduletest.server_local_cache,use_version=randomized_version)
    A.write_caches=[]
    A.produce_disabled=True
    A.run_if_haveto=False
    A.get()
    assert A._da_output_origin=="cache"
    print((A.resource_stats))
