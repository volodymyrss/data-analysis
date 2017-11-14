import pytest
from flask import url_for


#@pytest.mark.options(debug=False)
#def test_app(app):
#    assert not app.debug, 'Ensure the app not in debug mode'

def test_api_ping(client):
    res = client.get(url_for('status'))
    assert res.json == {'ping': 'pong'}

def test_app_list(client):
    factory_list=client.get(url_for('list',modules=",".join(["ddmoduletest"]))).json

    print(factory_list)
    assert len(factory_list)>0
    assert 'AAnalysis' in factory_list


def test_live_delegation(client):
    factory_r=client.get(url_for('produce',target="BAnalysis",modules="ddmoduletest")).json

    print(factory_r)

    assert factory_r['data']=='dataBdataA'

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

    print(excinfo.value.resources[0].identity.get_modules_loadable())

    print(excinfo.value.resources[0])



    #assert False

def test_multiple_resource_delegation(client):
    import dataanalysis.core as da
    import dataanalysis

    da.reset()
    da.debug_output()

    import ddmoduletest
    reload(ddmoduletest)
    ddmoduletest.cache.delegating_analysis.append("ClientDelegatableAnalysis.*")

    A=ddmoduletest.TwoCDInputAnalysis()

    with pytest.raises(dataanalysis.core.AnalysisDelegatedException) as excinfo:
        A.get()

    assert len(excinfo.value.resources)==2

    assert isinstance(excinfo.value.resources[0], dataanalysis.caches.resources.WebResource)
    assert isinstance(excinfo.value.resources[1], dataanalysis.caches.resources.WebResource)

    print(excinfo.value.resources[0].identity.get_modules_loadable())

    print(excinfo.value.resources)


def test_live_resource_delegation(client):
    import os
    import threading
    import dataanalysis.core as da
    import dataanalysis

    da.reset()
    da.debug_output()

    import ddmoduletest
    ddmoduletest.cache.delegating_analysis = "ClientDelegatableAnalysisA"

    A = ddmoduletest.ClientDelegatableAnalysisA()

    with pytest.raises(dataanalysis.core.AnalysisDelegatedException) as excinfo:
        A.get()

    assert len(excinfo.value.resources)==1

    print(client)

    #r = client.get(excinfo.value.resources[0].url)
    r=excinfo.value.resources[0].get()
    #r = client.get(excinfo.value.resources[0].url)
    print(r)

    R = r.json
    print(R)

    assert R['data'] == 'dataA'

    print(R.keys())
    print(R['resource_stats']['main_executed_on'])

    assert os.getpid() == R['resource_stats']['main_executed_on']['pid']
    assert threading.current_thread().ident == R['resource_stats']['main_executed_on']['thread_id']


def test_live_multiple_resource_delegation(client):
    import dataanalysis.core as da
    import dataanalysis

    da.reset()
    da.debug_output()

    import ddmoduletest
    reload(ddmoduletest)
    ddmoduletest.cache.delegating_analysis.append("ClientDelegatableAnalysis.*")

    A=ddmoduletest.TwoCDInputAnalysis()

    with pytest.raises(dataanalysis.core.AnalysisDelegatedException) as excinfo:
        A.get()

    assert len(excinfo.value.resources)==2

    assert isinstance(excinfo.value.resources[0], dataanalysis.caches.resources.WebResource)
    assert isinstance(excinfo.value.resources[1], dataanalysis.caches.resources.WebResource)

    print(excinfo.value.resources[0].identity.get_modules_loadable())

    print(excinfo.value.resources)