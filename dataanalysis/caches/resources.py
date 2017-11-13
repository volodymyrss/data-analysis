from dataanalysis.printhook import log
import urllib

from dataanalysis.caches.delegating import SelectivelyDelegatingCache, WaitingForDependency
from dataanalysis.printhook import log


# execution is also from cache?..

# no traces required in the network ( traces are kept for information )
# callbacks

class Resource(object):
    def __init__(self, hashe, identity):
        self.hashe=hashe
        self.identity=identity

class ResourceFactory(object):
    def find_resource(self,hashe,identity):
        return Resource(hashe,identity)

class WebResource(Resource):
    def __init__(self, hashe, identity, host, port, api_version):
        """
        :param hashe:
        :param identity:
         :type da.DataAnalysisIdentity
        :param host:
        :param port:
        :param api_version:
        """
        super(WebResource, self).__init__(hashe, identity)

        self.host=host
        self.port=port
        self.api_version=api_version

    @property
    def url(self):
        params=dict(
            target=self.identity.factory_name,
            modules=",".join(self.identity.get_modules_loadable()),
            #assumptions=self.identity.assumptions,
        )

        log("params",params)

        url_root="http://%(host)s:%(port)i/api/%(api_version)s/produce"%dict(
            host=self.host,
            port=self.port,
            api_version=self.api_version
        )

        return url_root+"?"+urllib.urlencode(params)

    def __repr__(self):
        return "[WebResource: %s for %s]"%(self.url,repr(self.identity))

class WebResourceFactory(object):
    host=None
    port=None
    api_version=None

    def find_resource(self,hashe,identity):
        return WebResource(hashe,identity, host=self.host, port=self.port, api_version=self.api_version)

class CacheDelegateToResources(SelectivelyDelegatingCache):
    resource_factory=None

    def delegate(self, hashe, obj):
        # here we attempt to find the resource provider

        resource = self.resource_factory().find_resource(hashe,obj.get_identity())

        raise WaitingForDependency(hashe, resource)

