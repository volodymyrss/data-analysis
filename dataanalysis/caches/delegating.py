import re

import dataanalysis.caches.cache_core
import dataanalysis.core as da
from dataanalysis.printhook import log


class DelegatedNoticeException(da.AnalysisDelegatedException):
    pass

class WaitingForDependency(da.AnalysisDelegatedException):
    def __init__(self, hashe, resources):
        self.hashe=hashe
        self.resources=resources


class DelegatingCache(dataanalysis.caches.cache_core.Cache):
    def delegate(self, hashe, obj):
        pass

    def find_content_hash_obj(self,hashe,obj):
        delegation_state=self.delegate(hashe, obj)
        raise da.AnalysisDelegatedException(hashe,origin=repr(self),delegation_state=delegation_state)

class SelectivelyDelegatingCache(DelegatingCache):
    delegating_analysis=None
    delegate_by_default=True

    def will_delegate(self,hashe,obj):
        log("trying for delegation",hashe,"in",self,level="top")

        if not getattr(obj,'_da_delegation_allowed',True):
            log("this object explicitly prohibits delegation",obj,level="top")
            return False

        if self.delegating_analysis is None:
            log("this cache has no delegation specifications, going for default:",self.delegate_by_default,level="top")
            return self.delegate_by_default

        if any([hashe[-1] == option or re.match(option,hashe[-1]) for option in self.delegating_analysis]):
            log("delegation IS allowed",level="top")
            return True
        else:
            log("delegation not chosen:",hashe[-1],self.delegating_analysis,level="top")
            return False

    def restore(self,hashe,obj,restore_config=None):
        if self.will_delegate(hashe, obj):
            return super(SelectivelyDelegatingCache, self).restore(hashe, obj, restore_config)
        else:
            return self.restore_from_parent(hashe, obj, restore_config)

