import datetime
import json

try:
    import urllib.parse
except ImportError:
    import urllib.parse as urlparse

import requests
import dqueue
import os

from dataanalysis.printhook import log, log_hook


class CallbackHook(object):
    def __call__(self, *args,**kwargs):
        level, obj=args
        message=kwargs['message']

        log("callback class:", self, "will send message \"", message, "\" for object", obj, "to callbacks ", obj.callbacks, level='callback')
        for callback_url in obj.callbacks:
            callback_filter=default_callback_filter
            if isinstance(callback_url,tuple):
                callback_url,callback_filter_name=callback_url #
                callback_filter=globals()[callback_filter_name]

            callback_class = callback_filter
            log("callback class:", callback_class, level='callback')
            callback = callback_class(callback_url)
            log("processing callback url", callback_url, callback, level='callback')

            r = callback.process_callback(
                    level=level,
                    obj=obj,
                    message=message,
                    data=kwargs
                )

            if r is not None:
                object_data=callback.extract_data(obj)
                object_data['request_root_node']=getattr(obj,'request_root_node',False)
                if 'hashe' in object_data:
                    object_data.pop('hashe')
                log("loghook from callback",level='callback')
                log_hook("callback",obj,level_orig=level,callback_params=callback.url_params, callback_response=r, callback_response_content="obsolete",**kwargs)


class Callback(object):
    callback_accepted_classes = None

    @classmethod
    def set_callback_accepted_classes(cls,classes):
        if cls.callback_accepted_classes is None:
            cls.callback_accepted_classes=[]

        for c in classes:
            if c not in cls.callback_accepted_classes:
                log("adding callback-accepted class",c,level="callback")
                cls.callback_accepted_classes.append(c)

        log("callback currently accepts classes",cls.callback_accepted_classes, level='callback')

    def __init__(self,url):
        self.url=url

        try:
            self.url_params=urllib.parse.parse_qs(urllib.parse.urlparse(self.url).query)
        except Exception as e:
            log("failed extracting callback parameters:",e,level='callback-debug')
            self.url_params={}
        log('created callback',self.url,level='callback-debug')
        log('extracted callback params',self.url_params,'from',self.url,level='callback-debug')

    def __repr__(self):
        return "[%s: %s]"%(self.__class__.__name__,self.url)

    def filter_callback(self,level,obj,message,data):
        if data.get('state','unknown') in ["failed"]:
            return True

        if self.callback_accepted_classes is None:
            log("callback  accepted:",message,level="callback")
            return True

        for accepted_class in self.callback_accepted_classes:
            try:
                if issubclass(obj.__class__, accepted_class):
                    return True
            except Exception as e:
                log("unable to filter",obj,obj.__class__,accepted_class)
                raise

        log("callback NOT accepted:",message,repr(obj),level="callback-debug")
        log("accepted callbacks:",self.callback_accepted_classes,level="callback-debug")
        return False

    def process_callback(self,level,obj,message,data):
        if self.filter_callback(level,obj,message,data):
            return self.process_filtered(level,obj,message,data)

    def extract_data(self,obj):
        if obj._da_locally_complete is not None:
            return dict(hashe=obj._da_locally_complete)
        return {}

    def process_filtered(self,level,obj,message,data):
        if os.environ.get("DDA_REDUCED_CALLBACKS", "no") == "yes":
            if message in ["restored from cache", "started object processing"]:
                log("\033[31mDDA_REDUCED_CALLBACKS requests ignoring callback\033[0m", level, obj, message, data, level="top")
                return

        if self.url is None:
            return

        object_data={}
        object_data.update(data)
        object_data.update(self.extract_data(obj))
        object_data['request_root_node'] = getattr(obj, 'request_root_node', False)

        params = dict(
            level=level,
            node=obj.get_signature(),
            message=message,
        )

        params.update(object_data)
        params['action'] = data.get('state', 'progress')

        for k,v in params.items():
            log(f"callback key {k} size {len(json.dumps(v))}", level='callback')

        if self.url.startswith("file://"):
            fn=self.url[len("file://"):]
            with open(fn,'a') as f:
                log("callback to file", self.url, fn, params, level="callback")
                f.write(str(datetime.datetime.now())+" "+level+": "+" in "+str(obj)+" got "+message+"; "+repr(object_data)+"\n")

        elif self.url.startswith("http://") or self.url.startswith("https://"):

            try:
                r = dqueue.from_uri().callback(
                            self.url,
                            params=params
                        )

                log("callback succeeded",self.url, params, r, level="callback")
                log_hook("callback",obj,message="callback succeeded",callback_url=self.url,callback_params=self.url_params,action_params=params,callback_response_content=r)
                return r
            except requests.ConnectionError as e:
                log("callback failed", self.url,params,":",e,level="callback")
                log_hook("callback",obj,message="callback failed!",callback_exception=repr(e),callback_url=self.url,callback_params=self.url_params,action_params=params)
                return "callback failed: " + repr(e)
        else:
            raise Exception("unknown callback method",self.url)


default_callback_filter=Callback
