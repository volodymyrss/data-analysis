
class FileBackend:
    def exists(self,fn):
        return os.path.exists(fn)

    def getsize(self,origin):
        return os.path.getsize(origin)/1024./1024.
    
    def get(self,orig,dest,gz=False):
        print(orig,"to",dest)
        if gz:
            open(dest,"w").write(gzip.open(orig).read())
        else:
            shutil.copy(orig,dest)
    
    def symlink(self,orig,dest):
        os.symlink(orig,dest)
    
    def put(self,orig,dest):
        shutil.copy(orig,dest)
    
    def makedirs(self,dirs):
        os.makedirs(dirs)

    def open(self,fn,mode="r",gz=False):
        if gz:
            return gzip.open(fn,mode)
        return open(fn,mode)

    def flush(self):
        pass

