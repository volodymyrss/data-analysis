import json
import urllib.request, urllib.parse, urllib.error

import requests

from dataanalysis.caches.delegating import SelectivelyDelegatingCache
from dataanalysis.core import AnalysisDelegatedException
from dataanalysis.printhook import log
from dataanalysis import hashtools


# execution is also from cache?..

# no traces required in the network ( traces are kept for information )
# callbacks

class GenericResourceException(Exception):
    pass

class Response(object):
    def __init__(self,status,data):
        self.status=status
        self.data=data


    @classmethod
    def from_response_json(cls,response_json,allow_exception=True):
        if 'status' not in response_json or 'data' not in response_json:
            raise Exception("response json does not have required \"status\" and \"data\" fields\n\n",response_json)

        self=cls(response_json['status'],response_json['data'])

        if allow_exception and self.status == "error":
            exception_name=self.data.get('exception_name','GenericResourceException')
            exception_data = self.data.get('exception_data', {})
            raise GenericResourceException(exception_name,exception_data,self.data)

        return self

    def jsonify(self):
        return dict(
            status=self.status,
            data=self.data,
        )

    def __repr__(self):
        return "[Response: %s]"%self.status



class Resource(object):
    def __init__(self, hashe, identity, requested_by):
        self.hashe=hashe
        self.identity=identity
        self.requested_by=requested_by

    def jsonify(self):
        return dict(name=self.hashe[-1])

class ResourceFactory(object):
    def find_resource(self,hashe,identity):
        return Resource(hashe,identity)

def jsonify(x):
    try:
        x=hashtools.hashe_replace_object(x, None, 'None')
    except:
        pass

    return json.loads(json.dumps(x))

class WebResource(Resource):
    def __init__(self, hashe, identity, request_route, host, port, api_version, getter=None, endpoint=None):
        """
        :param hashe:
        :param identity:
         :type da.DataAnalysisIdentity
        :param host:
        :param port:
        :param api_version:
        """
        super(WebResource, self).__init__(hashe, identity, request_route)

        self.host=host
        self.port=port
        self.api_version=api_version
        self.getter=getter
        self.endpoint=endpoint

    @property
    def url(self):
        return self.get_url()

    @property
    def url_base(self):
        if self.endpoint is not None:
            return self.endpoint
        else:
            return "http://%(host)s:%(port)i"%dict(
                host=self.host,
                port=self.port,
            )


    def get_url(self,**extra_parameters):
        params=dict(
            target=self.identity.factory_name,
            modules=",".join(self.identity.get_modules_loadable()),
            assumptions=json.dumps(self.identity.assumptions),
            requested_by=",".join(self.requested_by),
            expected_hashe=json.dumps(self.identity.expected_hashe),
            mode="interactive",
        )

        if extra_parameters is not None:
            params.update(extra_parameters)

        log("params",params)

        url_root = self.url_base
        url_path = "/api/%(api_version)s/produce"%dict(
            api_version = self.api_version
        )

        if isinstance(url_root,bytes):
            url_root = url_root.decode('utf-8')

        url_root+=url_path

        url=url_root + "?" + urllib.parse.urlencode(params)

        log("url:",url)
        return url

    def __repr__(self):
        return "[WebResource: %s for %s]"%(self.url,repr(self.identity))

    def get(self,getter=None,mode="interactive"):
        if getter is None:
            getter=self.getter
            if getter is None:
                getter = lambda x: requests.get(x).json()

        return Response.from_response_json(getter(self.get_url(mode=mode)))

    def delayed(self,getter=None):
        return self.get(mode="interactive", getter=getter)

    def fetch(self,getter=None):
        return self.get(mode="fetch", getter=getter)

    def jsonify(self):
        return dict(name=self.hashe[-1],url=self.url)

class WebResourceFactory(object):
    host=None
    port=None
    api_version=None
    endpoint=None

    getter=None

    def find_resource(self,hashe,identity,requested_by):
        return WebResource(hashe,identity, requested_by, host=self.host, port=self.port, api_version=self.api_version, getter=self.getter, endpoint=self.endpoint)

class CacheDelegateToResources(SelectivelyDelegatingCache):
    resource_factory=None
    delegation_mode="raise"

    def load_content(self, hashe, c):
        return c

    def find_content_hash_obj(self,hashe,obj):
        if self.will_delegate(hashe, obj):
            resource = self.resource_factory.find_resource(hashe, obj.get_identity(), requested_by=obj._da_requested_by)

            if self.delegation_mode == "raise":
                raise AnalysisDelegatedException(hashe=hashe, resources=[resource])
            elif self.delegation_mode == "interactive":
                r=resource.get()
                log("interactive resource response",r)
                if r.status!="result":
                    log("interactive resource status does not allow restore:", r.status)
                    log(r)
                    log(r.data)
                    raise AnalysisDelegatedException(hashe,
                                                     comment="interative resource status was unable to provide result for restore:" + repr(r.status),
                                                     resources=[resource])
                else:
                    log("interactive resource returned result", r)
                    data=resource.get().data
                    log("interactive resource returned data", data)
                    return data
            else:
                raise Exception("undefined delegation mode in the cache:" + self.delegation_mode)


    def delegate(self, hashe, obj):
        pass
        # here we attempt to find the resource provider




