import pytest
from flask import url_for


#@pytest.mark.options(debug=False)
#def test_app(app):
#    assert not app.debug, 'Ensure the app not in debug mode'

def test_api_ping(client):
    res = client.get(url_for('status'))
    assert res.json == {'ping': 'pong'}

def test_app_list(client):
    factory_list=client.get(url_for('list',modules=",".join(["ddmoduletest","ddosa"]))).json

    print(factory_list)
    assert len(factory_list)>0
    assert 'AAnalysis' in factory_list


def test_live_delegation(client):
    factory_list=client.get(url_for('produce',target="BAnalysis",modules="ddmoduletest")).json

    print(factory_list)

    assert factory_list['data']=='dataBdataA'

    assert len(factory_list)>0


def test_resource_delegation(client):
    import dataanalysis.core as da
    import dataanalysis

    da.reset()
    da.debug_output()

    import ddmoduletest
    ddmoduletest.cache.delegating_analysis="AAnalysis"

    A=ddmoduletest.AAnalysis()

    with pytest.raises(dataanalysis.caches.delegating.WaitingForDependency) as excinfo:
        A.get()

    assert isinstance(excinfo.value.resource, dataanalysis.caches.resources.WebResource)

    print(excinfo.value.resource.identity.get_modules_loadable())

    print(excinfo.value.resource)

    #assert False


def test_live_resource_delegation(client):
    import dataanalysis.core as da
    import dataanalysis

    da.reset()
    da.debug_output()

    import ddmoduletest
    ddmoduletest.cache.delegating_analysis="AAnalysis"

    A=ddmoduletest.AAnalysis()

    with pytest.raises(dataanalysis.caches.delegating.WaitingForDependency) as excinfo:
        A.get()

    assert isinstance(excinfo.value.resource, dataanalysis.caches.resources.WebResource)

    print(excinfo.value.resource.identity.get_modules_loadable())

    print(excinfo.value.resource)

    r=client.get(excinfo.value.resource.url)
    print(r)

    R=r.json
    print(R)

    assert R['data']=='dataA'

    #assert False

