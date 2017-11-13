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


def test_app_produce(client):
    factory_list=client.get(url_for('produce',target="BAnalysis",modules="ddmoduletest")).json

    print(factory_list)
    assert len(factory_list)>0

def test_app_delegation(client):
    factory_list=client.get(url_for('produce',target="BAnalysis",modules="ddmoduletest")).json

    print(factory_list)
    assert len(factory_list)>0


def test_live_delegation(client):
    factory_list=client.get(url_for('produce',target="BAnalysis",modules="ddmoduletest")).json

    print(factory_list)
    assert len(factory_list)>0


def test_resource_provider():
    pass

def test_resource_delegation():
    import dataanalysis.core as da

    da.reset()
    da.debug_output()
    import dataanalysis.caches.resources

    class TCache(dataanalysis.caches.resources.CacheDelegateToResources):
        delegating_analysis=["Analysis"]

    class Analysis(da.DataAnalysis):
        cache=TCache()
        cached=True

        def main(self):
            raise Exception("this should be delegated")

    A=Analysis()

    try:
        A.get()
    except dataanalysis.caches.delegating.WaitingForDependency as e:
        print(e)
        assert isinstance(e.resource, dataanalysis.caches.resources.Resource)

