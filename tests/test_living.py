from flask import url_for

#@pytest.mark.options(debug=False)
#def test_app(app):
#    assert not app.debug, 'Ensure the app not in debug mode'

def test_api_ping(client):
    res = client.get(url_for('status'))
    assert res.json == {'ping': 'pong'}

def test_app_list(client):
    factory_list=client.get(url_for('list')).json

    print(factory_list)
    assert len(factory_list)>0


def test_app_produce(client):
    factory_list=client.get(url_for('produce',target="BAnalysis")).json

    print(factory_list)
    assert len(factory_list)>0
