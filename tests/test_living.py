import os
import threading

import pytest
from flask import url_for
import imp


#@pytest.mark.options(debug=False)
#def test_app(app):
#    assert not app.debug, 'Ensure the app not in debug mode'

def test_api_ping(client):
    url=url_for('status',_external=True)
    print(("full url",url))
    res = client.get(url)
    assert res.json['ping']== 'pong'


def test_api_ping_separate(ddservice_fixture,app):
    url=url_for('status')
    import requests

    full_url = ddservice_fixture.decode("utf-8")+ url
    print(("full url", full_url))
    res = requests.get(full_url)
    assert res.json()['ping']== 'pong'
    assert res.json()['pid'] != os.getpid()
    print((res.json()['thread'], threading.current_thread().ident))
    assert res.json()['thread'] != threading.current_thread().ident


def test_app_list(client):
    factory_list=client.get(url_for('list',modules=",".join(["ddmoduletest"]))).json

    print(factory_list)
    assert len(factory_list)>0
    assert 'AAnalysis' in factory_list



def test_live_delegation(ddservice_fixture,app):
    import requests
    import dataanalysis.core as da
    da.reset()
    da.debug_output()

    r=requests.get(ddservice_fixture.decode("utf-8") + url_for('produce', target="BAnalysis", modules="ddmoduletest"))

    print((r.content))

    factory_r=r.json()

    print(factory_r)

    assert factory_r['data']['data']=='dataB_A:dataA'

    assert len(factory_r)>0

def test_live_delegation_assumptions(ddservice_fixture,app):
    import requests
    import dataanalysis.core as da
    da.reset()
    da.debug_output()

    r=requests.get(ddservice_fixture.decode("utf-8")+url_for('produce', target="BAnalysis", modules="ddmoduletest", assume="AAnalysis.assumed_data=\"clients\""))

    print((r.content))

    factory_r=r.json()

    print(factory_r)

    assert factory_r['data']['data']=='dataB_A:dataAassumed:clients'

    assert len(factory_r)>0

def test_resource_delegation(client):
    import dataanalysis.core as da
    import dataanalysis

    da.reset()
    da.debug_output()

    import ddmoduletest
    ddmoduletest.cache.delegating_analysis="AAnalysis"

    A=ddmoduletest.AAnalysis()

    with pytest.raises(dataanalysis.core.AnalysisDelegatedException) as excinfo:
        A.get()

    assert len(excinfo.value.resources)==1

    assert isinstance(excinfo.value.resources[0], dataanalysis.caches.resources.WebResource)

    print((excinfo.value.resources[0].identity.get_modules_loadable()))

    print((excinfo.value.resources[0]))



    #assert False

def test_multiple_resource_delegation(client):
    import dataanalysis.core as da
    import dataanalysis

    da.reset()
    da.debug_output()

    import ddmoduletest
    imp.reload(ddmoduletest)
    ddmoduletest.cache.delegating_analysis.append("ClientDelegatableAnalysis.*")

    A=ddmoduletest.TwoCDInputAnalysis()

    with pytest.raises(dataanalysis.core.AnalysisDelegatedException) as excinfo:
        A.get()

    assert len(excinfo.value.resources)==2

    assert isinstance(excinfo.value.resources[0], dataanalysis.caches.resources.WebResource)
    assert isinstance(excinfo.value.resources[1], dataanalysis.caches.resources.WebResource)

    print((excinfo.value.resources[0].identity.get_modules_loadable()))

    print((excinfo.value.resources))

@pytest.mark.skip(reason="not used")
def test_live_resource_delegation(client):
    import dataanalysis.core as da
    import dataanalysis

    #print(dir(client))
    #raise Exception(client.__module__)

    da.reset()
    da.debug_output()

    import ddmoduletest
    imp.reload(ddmoduletest)
    ddmoduletest.cache.delegating_analysis = ["ClientDelegatableAnalysisA"]

    A = ddmoduletest.ClientDelegatableAnalysisA1()

    with pytest.raises(dataanalysis.core.AnalysisDelegatedException) as excinfo:
        A.get()

    assert len(excinfo.value.resources)==1

    getter=lambda x:client.get(x).json

    fr=excinfo.value.resources[0].fetch(getter=getter)

    print(fr)
    print((fr.data))

    assert fr.status == 'not allowed to produce'

    R=excinfo.value.resources[0].get(getter=getter)

    print(R)

    assert R.data['data'] == 'dataA1'

    print((list(R.data.keys())))
 #   print(R.data['_da_resource_summary']['main_executed_on'])

#    assert os.getpid() == R.data['_da_resource_summary']['main_executed_on']['pid']
    #assert threading.current_thread().ident == R.data['_da_resource_summary']['main_executed_on']['thread_id']

@pytest.mark.skip(reason="not used")
def test_live_resource_server_cachable(client):
    import dataanalysis.core as da
    import dataanalysis

    da.reset()
    da.debug_output()

    import ddmoduletest
    imp.reload(ddmoduletest)
    ddmoduletest.cache.delegating_analysis = ["ServerCachableAnalysis"]

    A = ddmoduletest.ServerCachableAnalysis()
    A.cache=ddmoduletest.cache

    with pytest.raises(dataanalysis.core.AnalysisDelegatedException) as excinfo:
        A.get()

    assert len(excinfo.value.resources)==1

    getter=lambda x:client.get(x).json

    fr = excinfo.value.resources[0].get(getter=getter)

    assert fr.status == "result"
    print((fr.data))
    assert fr.data['data'] == 'dataAdataB'

    fr = excinfo.value.resources[0].fetch(getter=getter)

    assert fr.status == "result"

    print((fr.data))

    assert fr.data['data'] == 'dataAdataB'


def test_live_multiple_resource_delegation(client):
    import dataanalysis.core as da
    import dataanalysis

    da.reset()
    da.debug_output()

    import ddmoduletest
    imp.reload(ddmoduletest)
    ddmoduletest.cache.delegating_analysis.append("ClientDelegatableAnalysis.*")

    A=ddmoduletest.TwoCDInputAnalysis()

    with pytest.raises(dataanalysis.core.AnalysisDelegatedException) as excinfo:
        A.get()

    assert len(excinfo.value.resources)==2

    assert isinstance(excinfo.value.resources[0], dataanalysis.caches.resources.WebResource)
    assert isinstance(excinfo.value.resources[1], dataanalysis.caches.resources.WebResource)

    print((excinfo.value.resources[0].identity.get_modules_loadable()))

    print((excinfo.value.resources))


@pytest.mark.skip(reason="obsolete")
def test_live_chained_delegation(ddservice_fixture, app):
    import dataanalysis.core as da
    import dataanalysis

    da.reset()
    da.debug_output()

    import ddmoduletest
    imp.reload(ddmoduletest)
    ddmoduletest.cache.delegating_analysis.append("ChainedDelegator.*")

    A=ddmoduletest.ChainedDelegator()

    with pytest.raises(dataanalysis.core.AnalysisDelegatedException) as excinfo:
        A.get()

    assert len(excinfo.value.resources)==1

    assert isinstance(excinfo.value.resources[0], dataanalysis.caches.resources.WebResource)

def test_chained_waiting(ddservice_fixture, app):
    import dataanalysis.core as da

    da.reset()
    da.debug_output()

    import ddmoduletest
    imp.reload(ddmoduletest)

    ddmoduletest.cache.delegating_analysis.append("ChainedDelegator.*")
    ddmoduletest.cache.delegation_mode="interactive"

    ddmoduletest.cache.resource_factory.endpoint = ddservice_fixture
    #ddmoduletest.cache.resource_factory.getter=getter

    A=ddmoduletest.ChainedDelegator()

    with pytest.raises(da.AnalysisDelegatedException) as excinfo:
        a=A.get()

    assert len(excinfo.value.resources)==1
    assert excinfo.value.resources[0].hashe[-1] == "ChainedDelegator.v0"


@pytest.mark.skip(reason="obsolete")
def test_chained(ddservice_fixture, app):
    import dataanalysis.core as da

    da.reset()
    da.debug_output()

    import ddmoduletest
    imp.reload(ddmoduletest)

    ddmoduletest.cache.delegating_analysis.append("ChainedServerProducer.*")
    ddmoduletest.cache.delegation_mode="interactive"

    ddmoduletest.cache.resource_factory.endpoint = ddservice_fixture
    #ddmoduletest.cache.resource_factory.getter=getter

    A=ddmoduletest.ChainedServerProducer()
    A.produce_disabled=True

    a=A.get()

    assert a.data=={'a': 'dataA', 'b': 'dataB_A:dataA'}
    assert a.resource_stats_a['main_executed_on']['pid'] != os.getpid()
    assert a.resource_stats_b['main_executed_on']['pid'] != os.getpid()
    assert a.resource_stats_a['main_executed_on']['pid'] == a.resource_stats_b['main_executed_on']['pid']

@pytest.mark.skip(reason="obsolete")
def test_passing_assumptions(ddservice_fixture, app):
    import dataanalysis.core as da

    da.reset()
    da.debug_output()

    import ddmoduletest
    imp.reload(ddmoduletest)

    ddmoduletest.cache.delegating_analysis.append("ChainedServerProducer.*")
    ddmoduletest.cache.delegation_mode="interactive"

    ddmoduletest.cache.resource_factory.endpoint = ddservice_fixture
    #ddmoduletest.cache.resource_factory.getter=getter

    A=ddmoduletest.ChainedServerProducer(assume=[ddmoduletest.AAnalysis(use_assumed_data="fromclient")])
    A.produce_disabled=True

    a=A.get()


    print((a.data))

    assert a.data=={'a': 'dataAassumed:fromclient', 'b': 'dataB_A:dataAassumed:fromclient'}
    assert a.resource_stats_a['main_executed_on']['pid'] != os.getpid()
    assert a.resource_stats_b['main_executed_on']['pid'] != os.getpid()
    assert a.resource_stats_a['main_executed_on']['pid'] == a.resource_stats_b['main_executed_on']['pid']


@pytest.mark.skip(reason="obsolete")
def test_passing_unmanagable_assumptions(ddservice_fixture, app):
    import dataanalysis.core as da
    import dataanalysis.caches.resources

    da.reset()
    da.debug_output()

    import ddmoduletest
    imp.reload(ddmoduletest)

    ddmoduletest.cache.delegating_analysis.append("ChainedServerProducer.*")
    ddmoduletest.cache.delegation_mode="interactive"

    ddmoduletest.cache.resource_factory.endpoint = ddservice_fixture
    #ddmoduletest.cache.resource_factory.getter=getter

    A=ddmoduletest.ChainedServerProducer(assume=[ddmoduletest.AAnalysis(input_x=ddmoduletest.TwoCDInputAnalysis())])
    A.produce_disabled=True

    #with pytest.raises(dataanalysis.caches.resources.GenericResourceException) as excinfo:
    a=A.get()

    #assert 'mismatch' in excinfo.value.args[2]['comment']


