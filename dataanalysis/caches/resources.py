from dataanalysis.caches.delegating import SelectivelyDelegatingCache, WaitingForDependency

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

class CacheDelegateToResources(SelectivelyDelegatingCache):
    def delegate(self, hashe, obj):
        # here we attempt to find the resource provider

        resource = ResourceFactory().find_resource(hashe,obj.get_identity())

        raise WaitingForDependency(hashe, resource)

