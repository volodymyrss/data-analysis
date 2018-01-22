from dataanalysis.printhook import log
import datetime
import requests

class CallbackHook(object):
    def __call__(self, *args,**kwargs):
        level, obj=args
        message=kwargs['message']

        for callback_url in obj.callbacks:
            callback=Callback(callback_url)
            log("processing callback url", callback_url, callback)
            callback.process(level=level,obj=obj,message=message,data=kwargs)


class Callback(object):
    def __init__(self,url):
        self.url=url

    def __repr__(self):
        return "[%s: %s]"%(self.__class__.__name__,self.url)

    def process(self,level,obj,message,data):
        if self.url.startswith("file://"):
            fn=self.url[len("file://"):]
            with open(fn,'a') as f:
                f.write(str(datetime.datetime.now())+" "+level+": "+" in "+str(obj)+" got "+message+"; "+repr(data)+"\n")

        elif self.url.startswith("http://"):
            requests.get(self.url+"/"+data.get('state','status'),
                         params=dict(
                             level=level,
                             node=obj.get_signature(),
                             message=message,
                         ))
        else:
            raise Exception("unknown callback method",self.url)

