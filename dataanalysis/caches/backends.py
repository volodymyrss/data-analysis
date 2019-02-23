

import gzip
import os
import shutil
import subprocess

from dataanalysis.printhook import log

global_readonly_caches=False

def is_datafile(b):
# delayed import
    from .cache_core import DataFile
    return isinstance(b,DataFile)

def update_dict(a,b):
    return dict(list(a.items())+list(b.items()))

class FileBackend:
    def exists(self,fn):
        return os.path.exists(fn)

    def getsize(self,origin):
        return os.path.getsize(origin)/1024./1024.
    
    def get(self,orig,dest,gz=False):
        log(self,orig,"to",dest)
        if gz:
            open(dest,"wb").write(gzip.open(orig,"rb").read())
        else:
            shutil.copy(orig,dest)
    
    def symlink(self,orig,dest):
        os.symlink(orig,dest)
    
    def put(self,orig,dest):
        log("backend",self,"writing",orig,"to",dest)
        shutil.copy(orig,dest)
    
    def makedirs(self,dirs):
        try:
            os.makedirs(dirs)
        except OSError as e:
            log("backend failed makedirs:",e)

    def open(self,fn,mode="r",gz=False):
        if gz:
            return gzip.open(fn,mode)
        return open(fn,mode)

    def flush(self):
        pass


class IRODSFileBackend:
    def exists(self,fn):
        try:
            subprocess.check_call(["ils",fn])
            return True
        except subprocess.CalledProcessError:
            return False
        except:
            return False

    def getsize(self,origin):
        return 0 
        #return os.path.getsize(origin)/1024./1024.
    
    def get(self,orig,dest,gz=False):
           # shutil.copy(orig,dest)
        subprocess.check_call(["iget","-f",orig])
        if gz:
            open(dest,"w").write(gzip.open(os.path.basename(orig)).read())
    
    def symlink(self,orig,dest):
        os.symlink(orig,dest)
    
    def put(self,orig,dest):
        try:
            subprocess.check_call(["iput","-f",orig,dest])
        except:
            pass
    
    def makedirs(self,dirs):
        try:
            subprocess.check_call(["imkdir","-p",dirs])
        except:
            pass

    def open(self,fn,mode="r",gz=False):
        local_fn=os.path.basename(fn) # !!

        if "w"==mode:
            log("will later put file to irods")
            self.register_pending_put(local_fn,fn)
        elif "r"==mode:
            log("will get file from irods:",fn,local_fn)
            self.get(fn,local_fn)
        else:
            raise Exception("do not understand this mode: "+mode)

        if gz:
            return gzip.open(local_fn,mode)
        return open(local_fn,mode)

    def register_pending_put(self,local_fn,remote_fn):
        if self.pending_put is None:
            self.pending_put=[]
        self.pending_put.append([local_fn,remote_fn])

    pending_put=None

    def flush(self):
        while self.pending_put is not None and len(self.pending_put)>0:
            local,remote=self.pending_put.pop()
            self.put(local,remote)


class SSHFileBackend:
    # sshroot="apcclwn12:/Integral2/data/reduced/ddcache/"

    def exists(self, fn):
        try:
            subprocess.check_call(["scp", fn, "./"])
            return True
        except subprocess.CalledProcessError:
            return False

    def getsize(self, origin):
        return 0
        # return os.path.getsize(origin)/1024./1024.

    def get(self, orig, dest, gz=False):
        print(["scp", orig, dest], level="top")
        subprocess.check_call(["scp", orig, dest])
        if gz:
            open(dest, "w").write(gzip.open(os.path.basename(orig)).read())

    def symlink(self, orig, dest):
        os.symlink(orig, dest)

    def put(self, orig, dest):
        destdir = "/".join(dest.split("/")[:-1])

        print("makedirs", ["scp", "-r", orig, destdir], level="top")
        subprocess.check_call(["scp", "-r", orig, destdir])

    def makedirs(self, dirs):
        host, thedirs = dirs.split(":")
        subprocess.check_call(["ssh", host, "mkdir -pv " + thedirs])

    def register_pending_put(self, local_fn, remote_fn):
        if self.pending_put is None:
            self.pending_put = []
        self.pending_put.append([local_fn, remote_fn])

    pending_put = None

    def flush(self):
        while self.pending_put is not None and len(self.pending_put) > 0:
            local, remote = self.pending_put.pop()
            self.put(local, remote)

    def open(self, fn, mode="r", gz=False):
        local_fn = os.path.basename(fn)  # !!

        if "w" == mode:
            log("will later put file to ssh", local_fn, fn)
            self.register_pending_put(local_fn, fn)
        elif "r" == mode:
            log("will get file from ssh:", fn, local_fn)
            self.get(fn, local_fn)
        else:
            raise Exception("do not understand this mode: " + mode)

        if gz:
            return gzip.open(local_fn, mode)
        return open(local_fn, mode)
