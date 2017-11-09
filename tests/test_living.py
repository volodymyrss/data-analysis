from flask import url_for

#@pytest.mark.options(debug=False)
#def test_app(app):
#    assert not app.debug, 'Ensure the app not in debug mode'

def test_api_ping(client):
    res = client.get(url_for('ping'))
    assert res.json == {'ping': 'pong'}

def test_app(client):
    assert client.get(url_for('run')).status_code == 200
    assert client.get(url_for('list')).status_code == 200