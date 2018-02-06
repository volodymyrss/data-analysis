import glob
import os
import random
import subprocess
import sys

import yaml
import json

package_root=os.path.dirname(os.path.dirname(__file__))

sys.path.insert(0,package_root)
sys.path.insert(0,package_root+"/tests")

import dataanalysis.core as da

rundda_path=package_root+"/tools/rundda.py"

env = os.environ
env['PYTHONPATH'] = package_root + "/tests:" + env.get('PYTHONPATH','')



def test_simple():
    cmd=[
        'python',rundda_path,
        'ClientDelegatableAnalysisA',
        '-m','ddmoduletest',
        '-a','ddmoduletest.ClientDelegatableAnalysisA(use_sleep=0.2,use_cache=ddmoduletest.server_local_cache)',
        '-f','ClientDelegatableAnalysisA',
    ]
    p=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,env=env)
    p.wait()
    print(p.stdout.read())

    p = subprocess.Popen(cmd[:-2], stdout=subprocess.PIPE, stderr=subprocess.STDOUT,env=env)
    p.wait()
    print(p.stdout.read())


def test_prompt_delegation():
    queue_dir="tmp.queue"

    randomized_version="v%i"%random.randint(1,10000)

    injection_fn = "injection.json"
    json.dump(['ClientDelegatableAnalysisA',
               {
                   "data_add": "added"
               }
               ],
              open(injection_fn,"w"),
              )

    cmd=[
        'python',rundda_path,
        'ClientDelegatableAnalysisA',
        '-m','ddmoduletest',
        '-a','ddmoduletest.ClientDelegatableAnalysisA(use_sleep=0.2,use_cache=ddmoduletest.server_local_cache,use_version="%s")'%randomized_version,
        '-i',injection_fn,
        '-V','-x',
        '-D',queue_dir,
        '--callback', 'http://test/callback',
    ]

    for fn in glob.glob(queue_dir+"/waiting/*"):
        os.remove(fn)

    exception_report="exception.yaml"
    if os.path.exists(exception_report):
        os.remove(exception_report)

    # run it
    p=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,env=env)
    p.wait()
    print(p.stdout.read())

    assert os.path.exists(exception_report)
    recovered_exception=yaml.load(open(exception_report))

    print(recovered_exception)

    jobs=(glob.glob(queue_dir+"/waiting/*"))
    assert len(jobs)==1

    job=yaml.load(open(jobs[0]))

    print(job)


    from dataanalysis.caches.queue import QueueCacheWorker
    qw=QueueCacheWorker(queue_dir)
    print(qw.queue.info)
    assert qw.queue.info['waiting']==1

    da.debug_output()
    qw.run_once()

    A = da.byname('ClientDelegatableAnalysisA')
    assert A.sleep == 0.2
    assert A.version == randomized_version

    print("recovered object:", A)

    jobs = (glob.glob(queue_dir + "/waiting/*"))
    assert len(jobs) == 0

    # run again, expecting from cache
    exception_report = "exception.yaml"
    if os.path.exists(exception_report):
        os.remove(exception_report)

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,env=env)
    p.wait()
    print(p.stdout.read())

    import ddmoduletest

    da.debug_output()

    A=ddmoduletest.ClientDelegatableAnalysisA(use_sleep=0.2,use_cache=ddmoduletest.server_local_cache,use_version=randomized_version)
    A.write_caches=[]
    A.produce_disabled=True
    A.run_if_haveto=False
    A.get()
    assert A._da_output_origin=="cache"
    assert A.data=="dataAadded"
    print(A.resource_stats)

def test_delegation():
    queue_dir="tmp.queue"

    randomized_version="v%i"%random.randint(1,10000)
    callback_file = "./callback"




    cmd=[
        'python',rundda_path,
        'ClientDelegatableAnalysisA',
        '-m','ddmoduletest',
        '-a','ddmoduletest.ClientDelegatableAnalysisA(use_sleep=0.2,use_cache=ddmoduletest.server_local_cache,use_version="%s")'%randomized_version,
        '-Q',queue_dir,
        '--callback','file://'+callback_file,
    ]

    for fn in glob.glob(queue_dir+"/waiting/*"):
        os.remove(fn)

    if os.path.exists(callback_file):
        os.remove(callback_file)

    exception_report="exception.yaml"
    if os.path.exists(exception_report):
        os.remove(exception_report)

    assert not os.path.exists(callback_file)

    # run it
    p=subprocess.Popen(cmd,stdout=subprocess.PIPE,stderr=subprocess.STDOUT,env=env)
    p.wait()
    print(p.stdout.read())

    assert os.path.exists(exception_report)
    recovered_exception = yaml.load(open(exception_report))

    print(recovered_exception)


    jobs=(glob.glob(queue_dir+"/waiting/*"))
    assert len(jobs)==1

    job=yaml.load(open(jobs[0]))

    print(job)


    from dataanalysis.caches.queue import QueueCacheWorker
    qw=QueueCacheWorker(queue_dir)
    print(qw.queue.info)
    assert qw.queue.info['waiting']==1

    qw.run_once()

    assert os.path.exists(callback_file)
    callback_info = open(callback_file).readlines()
    print(callback_info)
    assert len(callback_info) == 5


    jobs = (glob.glob(queue_dir + "/waiting/*"))
    assert len(jobs) == 0


    # run again, expecting from cache
    exception_report = "exception.yaml"
    if os.path.exists(exception_report):
        os.remove(exception_report)

    p = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,env=env)
    p.wait()
    print(p.stdout.read())

    assert not os.path.exists(exception_report)

    import ddmoduletest

    da.debug_output()

    A=ddmoduletest.ClientDelegatableAnalysisA(use_sleep=0.2,use_cache=ddmoduletest.server_local_cache,use_version=randomized_version)
    A.write_caches=[]
    A.produce_disabled=True
    A.run_if_haveto=False
    A.get()
    assert A._da_output_origin=="cache"
    print(A.resource_stats)
