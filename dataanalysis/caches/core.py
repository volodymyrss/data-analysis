from __future__ import print_function

import cPickle
import copy
import gzip
import os
import pprint
import shutil
import socket
import sqlite3 as lite
import subprocess
import time

import analysisfactory
from bcolors import render

from dataanalysis import hashtools
from dataanalysis.printhook import print, cprint

from dataanalysis import  caches
from dataanalysis.caches import backends

global_readonly_caches=False

def is_datafile(b):
# delayed import
    from dataanalysis.core import DataFile
    return isinstance(b,DataFile)

def update_dict(a,b):
    return dict(a.items()+b.items())

class Cache(object): #d
    # currently store each cache in a file; this is not neccesary 
   # __metaclass__ = decorate_all_methods

    cache={}
    filecacheroot="./filecache"

    parent=None

    readonly_cache=False

    filebackend=caches.backends.FileBackend()

    can_url_to_cache=True
 
    def reset(self):
        cprint("resetting cache PLACEHOLDER",self)

    def reset_stack(self):
        self.reset()
        if self.parent is not None:
                self.parent.reset()

    def statistics(self):
        pass

    def hashe2signature(self,hashe_raw):
        hashe= hashtools.hashe_replace_object(hashe_raw, None, "None")
        cprint("hashe:",hashe)
        if isinstance(hashe,tuple):
            if hashe[0]=="analysis":
                return hashe[2]+":" + hashtools.shhash(hashe)[:8]
        sig= hashtools.shhash(hashe)[:8]
        cprint("signature hashe:",sig)
        return sig

    def __init__(self,rootdir=None):
        if rootdir is not None:
            self.filecacheroot=rootdir

    def __repr__(self):
        return "["+self.__class__.__name__+" of size %i at %s]"%(len(self.cache.keys()),self.filecacheroot)

    def find(self,hashe):
        #for c in self.cache.keys():
            #cprint("cache:",c)

        self.load()

        if hashe in self.cache:
            fi=self.cache[hashe]
            cprint("{log:cache}","cache found!",fi)
            return fi
            
        cprint("found no cache for",hashe)

        return None
    
    def runtime_update(self,obj,c):
        pass
        #if self.parent is not None:
        #    self.parent.runtime_unregister(obj)
    
    def restore_from_parent(self,hashe,obj,rc=None):
        if self.parent is None:
            cprint("no parent available to call for")
            return None
        
        cprint("there is a parent available to call for:",self.parent)
        from_parent=self.parent.restore(hashe,obj,rc)
            
        if from_parent is not None:
            print("storing what restored from parent")
            self.store(hashe,obj)

        return from_parent

    def store_to_parent(self,hashe,obj):
        if self.parent is None:
            cprint("no parent available to call for")
            return None
        
        cprint("there is a parent available to call for:",self.parent)
        return self.parent.store(hashe,obj)


    def load_content(self,hashe,c):
        cached_path=self.construct_cached_file_path(hashe,c)
                    
        cprint("restoring from",cached_path+"/cache.pickle.gz")

        try:
            cprint("loading from pickle")
            content=cPickle.load(self.filebackend.open(cached_path+"/cache.pickle.gz",gz=True))
            cprint("done loading from pickle")
            return content
        except TypeError as e:
            cprint("typeerror! "+repr(e))
            raise
        except IOError,cPickle.UnpicklingError:
            cprint("problem loading cache! corrupt cache!")
            raise
        except Exception:
            cprint("problem loading cache! corrupt cache!")
            raise

    def restore_file(self,origin,dest,obj,hashe):
       # statistics 
        cprint("restore file:")
        print("< ",origin,'{log:top}',level='top')
        print("> ",dest,'{log:top}',level='top')
        
        dest_unique=dest+"."+self.hashe2signature(hashe)
        print("> ",dest_unique,'{log:top}',level='top')
        
        print("as",dest_unique,level='top')

        fsize=self.filebackend.getsize(origin)/1024./1024.
        cprint("restoring file of",fsize,'{log:resources}','{log:cache}')

        t0=time.time()

        if dest.endswith(".gz"):
            self.filebackend.get(origin,dest_unique,gz=False)
        else:
            self.filebackend.get(origin,dest_unique,gz=True)

        tspent=time.time()-t0

        cprint("restoring took",tspent,"seconds, speed",fsize/tspent,'MB/s','{log:resources}','{log:cache}')

        tspentc=0

        cprint("here should verify integrity")
        cprint("successfully restored:",dest_unique)

        if os.path.exists(dest):
            cprint("destination exists:",dest)

        shutil.copyfile(dest_unique,dest)

        if obj.test_files:
            self.test_file(dest)
        
        cprint("successfully copied to",dest)

        return {'size':fsize,'copytime':tspent,'compressiontime':tspentc},dest_unique

# this should be elsewhere!
    def test_file(self,fn):
        if fn.endswith('fits') or fn.endswith('fits.gz'):
            print("fits, will test it")
            from astropy.io import fits as pyfits
            try:
                pyfits.open(fn)
            except Exception as e:
                print("corrupt fits file",fn,e)
                raise Exception('corrupt fits file in cache: '+fn)
        
        if fn.endswith('npy'):
            print("npy, will test it")
            import numpy
            try:
                numpy.load(fn)
            except:
                print("corrupt npy file",fn)
                raise Exception('corrupt fits file in cache: '+fn)
        
        if fn.endswith('npy.gz'):
            print("npy.gz, will test it")
            import numpy
            try:
                numpy.load(gzip.open(fn))
            except:
                print("corrupt npy.gz file",fn)
                raise Exception('corrupt fits file in cache: '+fn)

    def store_file(self,origin,dest):
        cprint("store file:")
        cprint("< ",origin,'{log:top}')
        cprint("> ",dest,'{log:top}')

        fsize=self.filebackend.getsize(origin)/1024./1024.
        cprint("storing file of",fsize,'{log:resources}','{log:cache}')

# note that this leaves files!!

        t0=time.time()
        if not origin.endswith(".gz"):
            origin_gzipped=origin+".gz"
            dest_gzipped=dest+".gz"
            os.system("gzip -c %s > %s"%(origin,origin_gzipped))
        else:
            origin_gzipped=origin
            dest_gzipped=dest

        #check_call(['gzip','-f',origin])
        tspentc=time.time()-t0

        if tspentc>0:
            cprint("compressing took",tspentc,"seconds, speed",fsize/tspentc,'MB/s','{log:resources}','{log:cache}')

        t0=time.time()

        self.filebackend.put(origin_gzipped,dest_gzipped) # just by name? # gzip optional
        #shutil.copyfile(origin+".gz",dest+".gz") # just by name? # gzip optional
        tspent=time.time()-t0

        cprint("storing took",tspent,"seconds, speed",fsize/tspent,'MB/s','{log:resources}','{log:cache}')

        return {'size':fsize,'copytime':tspent,'compressiontime':tspentc}

    def restore(self,hashe,obj,restore_config=None):
        # check if updated
        if obj.run_for_hashe or obj.mutating:
            return 

        if not any([isinstance(self,c) for c in obj.read_caches]):
            cprint("cache "+repr(self)+" should not be read by this analysis, allowed: "+repr(obj.read_caches))
            from_parent=self.restore_from_parent(hashe,obj,restore_config)
            return from_parent

        if restore_config is None:
            restore_config={}
        restore_config_default=dict(datafile_restore_mode="copy",datafile_target_dir=None) # no string
        restore_config=dict(restore_config_default.items()+(restore_config.items() if restore_config is not None else []))

        print("will restore",self,obj,"restore_config",restore_config)

        if obj.datafile_restore_mode is not None:
            restore_config['datafile_restore_mode']=obj.datafile_restore_mode

        c=self.find(hashe)
        if c is None:
            cprint("normal restore failed.")
            return self.restore_from_parent(hashe,obj,restore_config)

        cprint("requested to restore cache")
        cached_path=self.construct_cached_file_path(hashe,obj)
        cprint("cached path:",cached_path)

        obj._da_cache_path_root=cached_path
        obj._da_cached_pathes=[cached_path]

        try:
            c=self.load_content(hashe,c)
        except Exception as e:
            cprint("can not load content from cache, while cache record exists! inconsistent cache!") #???
            #raise Exception("can not copy from from cache, while cache record exists! inconsistent cache!") # ???
            return self.restore_from_parent(hashe,obj,restore_config)

        if not self.can_url_to_cache:
            print("cache can not be url, will copy all output",level="cache")
            restore_config['datafile_restore_mode']="copy"

        if obj.only_verify_cache_record:
            print("established cache record: will not acutally restore",level="cache")
            return True

        if isinstance(c,dict):
            for a,b in c.items(): 
                if is_datafile(b):
                    cprint("requested to restore DataFile",b,"mode",restore_config['datafile_restore_mode'],'{log:top}')

                    prefix=restore_config['datafile_target_dir']
                    if prefix is not None:
                        prefix=prefix+"/"
                        try:
                            os.makedirs(prefix)
                        except:
                            pass
                    else:
                        prefix=""

                    try:
                        if os.path.basename(b.path).endswith(".gz"):
                            stored_filename=cached_path+os.path.basename(b.path) # just by name? # gzip optional
                        else:
                            stored_filename=cached_path+os.path.basename(b.path)+".gz" # just by name? # gzip optional
                        print("stored filename:",stored_filename)
                    except Exception as e:
                        cprint("wat",e)
                    if not self.filebackend.exists(stored_filename): # and
                        cprint("file from cache does not exist, while cache record exists! inconsistent cache!") #,stored_filename)
                        return None

                    b.cached_path_valid_url=False
                    # other way
                    if restore_config['datafile_restore_mode']=="copy":
                        try:
                        #    print("attempt to restore file",stored_filename,b,id(b),level='top')
 #                           stored_filename=cached_path+os.path.basename(b.path)+".gz" # just by name? # gzip optional

                            if not self.filebackend.exists(stored_filename):
                                cprint("can not copy from from cache, while cache record exists! inconsistent cache!",stored_filename,level='top')
                                #raise Exception("can not copy from from cache, while cache record exists! inconsistent cache!")
                                # just reproduce?
                                return None

                            cprint("stored file:",stored_filename,"will restore as",prefix,b.path,level='top') 

                            b.restore_stats,restored_file=self.restore_file(stored_filename,prefix+os.path.basename(b.path),obj,hashe)
                            print("restored as",restored_file)

                            obj.note_resource_stats({'resource_type':'cache','resource_source':repr(self),'filename':b.path,'stats':b.restore_stats,'operation':'restore'})
                            b._da_unique_local_path=restored_file
                            b.restored_path_prefix=os.getcwd()+"/"+prefix
                        
                            print("note unique file name",b._da_unique_local_path)

                        except IOError:
                            if IOError.errno==20:
                                cprint("can not copy from from cache, while cache record exists! inconsistent cache!")
                      #          raise Exception("can not copy from from cache, while cache record exists! inconsistent cache!")
                                # just reproduce?
                                return None
                        except subprocess.CalledProcessError:
                            cprint("can not copy from from cache, while cache record exists! inconsistent cache!")
                 #           raise Exception("can not copy from from cache, while cache record exists! inconsistent cache!")
                            # just reproduce?
                            return None
                        except Exception as e:
                            cprint("UNHANDLED can not copy from from cache, while cache record exists! inconsistent cache!")
                            cprint("details:"+repr(e))
                 #           raise Exception("can not copy from from cache, while cache record exists! inconsistent cache!")
                            # just reproduce?
                            return None
                    elif restore_config['datafile_restore_mode']=="symlink":
                        self.filebackend.symlink(stored_filename,prefix+os.path.basename(b.path)+".gz") # just by name? # gzip optional
                    elif restore_config['datafile_restore_mode']=="urlfile":
                        b.cached_path_valid_url=True
                        open(prefix+os.path.basename(b.path)+".url.txt","w").write(stored_filename+"\n") # just by name? # gzip optional
                    elif restore_config['datafile_restore_mode']=="urlfileappend":
                        open(prefix+os.path.basename(b.path)+".urls.txt","a").write(stored_filename+"\n") # just by name? # gzip optional
                        b.cached_path_valid_url=True
                    elif restore_config['datafile_restore_mode']=="url_in_object":
                        b.cached_path=stored_filename # just by name? # gzip optional
                        if not os.path.exists(b.cached_path):
                            raise Exception("cached file does not exist!")
                        b.cached_path_valid_url=True
                        print("stored url:",b.cached_path,b.cached_path_valid_url)

                        if 'test_files' in restore_config and restore_config['test_files']:
                            try:
                                self.test_file(b.cached_path)
                            except:
                                return 
                    else:
                        raise Exception("datafile restore mode not understood!")

                    b.cached_path=stored_filename
                    b.restored_mode=restore_config['datafile_restore_mode']

            for k,i in c.items():
                setattr(obj,k,i)
            obj._da_recovered_restore_config=copy.copy(restore_config)

            cprint("restored with",obj._da_recovered_restore_config)
    
            try:
                a=obj.verify_content()
                if a is None:
                    raise Exception("returned none")
            except Exception as e:
                print("verify_content failed",e)
                return

            return True
        raise Exception("content from cache is not dict! "+str(c))


    # TODO: separate directory writing, to allow bundling
    def store(self,hashe,obj):
        if obj.run_for_hashe or obj.mutating:
            return 
        
        if self.readonly_cache:
            return self.store_to_parent(hashe,obj)

        if global_readonly_caches:
            raise Exception("all caches are readonly!")

        cprint("requested to store:",hashe)
        if not obj.cached:
            cprint("the object is declared as non-cached, not storing")
            return
        else:
            cprint("object",obj,"is cached, storing")
        
        if any([isinstance(self,c) for c in obj.write_caches]):

            cprint("storing:",hashe)

            if not self.filebackend.exists(self.filecacheroot):
                self.filebackend.makedirs(self.filecacheroot)

            obj._da_stamp=obj.get_stamp() # or in the object?
            
            if not hasattr(self,'cache'):
                self.cache={}

            content=obj.export_data()

            #cprint("content:",content)
                        
            cached_path=self.construct_cached_file_path(hashe,obj)

            obj._da_cached_path=cached_path

            if not hasattr(obj,'_da_cached_pathes'):
                obj._da_cached_pathes=[]
            obj._da_cached_pathes.append(cached_path)

            cprint("storing in",cached_path)
                        
            dn=os.path.dirname(cached_path)
            if not self.filebackend.exists(dn):
                self.filebackend.makedirs(dn)


            cPickle.dump(content,self.filebackend.open(cached_path+"cache.pickle.gz","w",gz=True))
            cPickle.dump(hashe,self.filebackend.open(cached_path+"hash.pickle.gz","w",gz=True))
            self.filebackend.open(cached_path+"hash.txt","w").write(pprint.pformat(hashe)+"\n")
            self.filebackend.open(cached_path+"log.txt.gz","w",gz=True).write(obj._da_main_log_content)
            
            aliases=analysisfactory.AnalysisFactory.list_relevant_aliases(obj)

            try:
                if aliases!=[]:
                    open(cached_path+"aliases.txt","w").write("\n".join([("="*80)+"\n"+pprint.pformat(a)+"\n"+("-"*80)+"\n"+pprint.pformat(b)+"\n" for a,b in aliases]))
            except:
                pass # fix!
            
            definitions=analysisfactory.AnalysisFactory.get_definitions()
            try:
                if definitions!=[]:
                    open(cached_path+"definitions.txt","w").write("\n".join([("="*80)+"\n"+pprint.pformat(a)+"\n"+("-"*80)+"\n"+pprint.pformat(b)+"\n" for a,b in definitions]))
            except:
                pass # fix!
                
            modules=analysisfactory.AnalysisFactory.get_module_description()
            filtered_modules=[]
            for m in reversed(modules):
                if m not in filtered_modules:
                    filtered_modules.append(m)
            self.filebackend.open(cached_path+"modules.txt","w").write(pprint.pformat(filtered_modules)+"\n")

            self.filebackend.flush()

            if hasattr(obj,'alias'):
                cprint('object has alias:',obj.alias)
                open(cached_path+"alias.txt","w").write(pprint.pformat(obj.alias)+"\n")
            else:
                cprint('object has no alias')


            if isinstance(content,dict):
                for a,b in content.items(): 
                    if is_datafile(b):
                        cprint("requested to store DataFile",b)

                        try:
                            p=cached_path+os.path.basename(b.path)
                        except Exception as e:
                            print("failed:",e)
                            print("path:",b.path)
                            print("b:",b)
                            raise
                        b.cached_path=p+".gz" if not p.endswith(".gz") else p
                        b.store_stats=self.store_file(b.path,p)
                        b._da_cached_path=cached_path
                        b.cached_path_valid_url=True
                        obj.note_resource_stats({'resource_type':'cache','resource_source':repr(self),'filename':b.path,'stats':b.store_stats,'operation':'store'})

            import socket

            cprint("will check if record exists",'{log:top}')
            found=self.find(hashe)
            if found is None:
                self.make_record(hashe,{'host':socket.gethostname(),'recored_at':time.time(),'content':content})
            else:
                #cprint("record already found:",found,'{log:reflections}')
                cprint("these results will be ignored! (why would we do this?..)","{log:reflections}") # current behavior is to replace
                self.make_record(hashe,{'host':socket.gethostname(),'recored_at':time.time(),'content':content}) # twice same!
            
        else:
            cprint("cache "+repr(self)+" should not be written by this analysis, allowed: "+repr(obj.write_caches))

        return self.store_to_parent(hashe,obj)
        
        # and save

    def make_record(self,hashe,content):
        #cprint("make record",hashe,content)
        self.cache[hashe]=content
        self.save()
        cprint("now entries",len(self.cache))

    def runtime_update(self,hashe,content):
        pass
        #self.make_record(self,hashe,content)

    def construct_cached_file_path(self,hashe,obj):
        cprint("requested default cached file path")

        def hash_to_path(hashe):
            if isinstance(hashe,tuple):
                if hashe[0]=="analysis": # more universaly
                    return hash_to_path(hashe[2])+"/"+hash_to_path(hashe[1])
                if hashe[0]=="list": # more universaly
                    return "..".join(map(hash_to_path,hashe[1:]))
                raise Exception("unknown tuple in the hash:"+str(hashe))
            if isinstance(hashe,str):
                return hashe
            raise Exception("unknown class in the hash:"+str(hashe))

        def hash_to_path2(hashe):
            #by32=lambda x:x[:8]+"/"+by8(x[8:]) if x[8:]!="" else x
            return hashe[2]+"/" + hashtools.shhash(repr(hashe[1]))

        return self.filecacheroot+"/"+hash_to_path2(hashe)+"/" # choose to avoid overlapp

        #return self.filecacheroot+"/"+hashe+"/"+os.path.basename(datafile.path)

    def save(self,target=None):
        if target is None:
            target=self.filecacheroot+"/index.pickle.gz"
        cPickle.dump(self.cache,gzip.open(target,"w"))

    def load(self,target=None):
        if target is None:
            target=self.filecacheroot+"/index.pickle.gz"

        if os.path.exists(target):
            self.cache=cPickle.load(gzip.open(target))
        else:
            cprint("file to load does not exist:",target)

    def make_delegation_record(self,hashe,module_description,dependencies):
        return self.parent.make_delegation_record(hashe,module_description,dependencies)

    def register_delegation(self,obj,hashe):
        cprint("requested to register delegation of",obj)
        cprint("hashe:",obj)

        cprint("modules used in dda factory:")
        
        module_description=analysisfactory.AnalysisFactory.get_module_description()

        dependencies=obj._da_delegated_input

        print ("will store hashe       ",hashe)
        print ("will store modules     ",module_description,dependencies)
        print ("will store dependencies",dependencies)

        return self.make_delegation_record(hashe,module_description,dependencies)
    
    def report_analysis_state(self,obj,state):
        state_dir_root=self.filecacheroot+"/state_reports/"
        state_dir=state_dir_root+"/"+obj.get_signature()
        
        try:
            os.makedirs(state_dir)
        except os.error:
            print("unable to create state dir!") # exissst?

        state_ticket_fn=repr(obj)+"_"+time.strftime("%Y%m%d_%H%M%S")+".txt"
        state_ticket_fn=state_ticket_fn.replace("]","")
        state_ticket_fn=state_ticket_fn.replace("[","")
        state_ticket_fn=state_ticket_fn.replace(":","_")

        f=open(state_dir+"/"+state_ticket_fn,"w")
        f.write("-"*80+"\n")
        f.write(repr(state)+"\n\n")
        f.write(socket.gethostname()+"\n\n")
        f.write(time.strftime("%Y-%m-%dT%H:%M:%S %a %d %B")+"\n\n")

        if hasattr(obj,'_da_requested_by'):
            f.write("requested by: "+" ".join(obj._da_requested_by)+"\n\n")

        if hasattr(obj,'_da_expected_full_hashe'):
            f.write("expected as: "+repr(obj._da_expected_full_hashe)+"\n\n")
 

        try:
            f.write("factory knows: "+repr(analysisfactory.AnalysisFactory.cache)+"\n\n")
        except Exception as e:
            print(e)
        

    def report_exception(self,obj,e):
        exception_dir_root=self.filecacheroot+"/exception_reports/"
        exception_dir=exception_dir_root+"/"+obj.get_signature()
        
        try:
            os.makedirs(exception_dir)
        except os.error:
            print("unable to create exception dir!") # exissst?

        exception_ticket_fn=repr(obj)+"_"+time.strftime("%Y%m%d_%H%M%S")+".txt"
        exception_ticket_fn=exception_ticket_fn.replace("]","")
        exception_ticket_fn=exception_ticket_fn.replace("[","")
        exception_ticket_fn=exception_ticket_fn.replace(":","_")

        try:
            f=open(exception_dir+"/"+exception_ticket_fn,"w")
            f.write("-"*80+"\n")
            f.write(repr(e)+"\n\n")
            f.write(socket.gethostname()+"\n\n")
            f.write(time.strftime("%Y-%m-%dT%H:%M:%S %a %d %B")+"\n\n")
            if hasattr(obj,'_da_requested_by'):
                f.write("requested by: "+" ".join(obj._da_requested_by)+"\n\n")

            try:
                f.write("factory knows: "+repr(analysisfactory.AnalysisFactory.cache)+"\n\n")
            except Exception as e:
                print(e)
        except Exception:
            print("unable to write exception!")
            print(e)


        # check
        #for m in sys.modules:
        #    cprint("all modules:",m,sys.modules[m],sys.modules[m].__file__ if hasattr(sys.modules[m],'__file__') else "no file??")


        

class CacheSqlite(Cache):
    cache={}

    def statistics(self):
        if self.con is None:
            cprint("NOT connected")
        else:
            cprint("connected to",self.con)

    def connect(self):
        if self.con is None:
            cprint("connecting to",self.filecacheroot+'/index.db')
            self.con = lite.connect(self.filecacheroot+'/index.db',1000)
        return self.con

    def __init__(self,*a,**aa):
        cprint(a,aa)
        super(CacheSqlite, self).__init__(*a, **aa)
        self.con=None
        #self.connect()

    def list(self,select=None,nlast=None):

        con=self.connect()
        cprint("listing cache")

        selection_string=""
        if select is not None:
            selection_string=" WHERE "+select # must be string

        nlast_string=""
        if nlast is not None:
            nlast_string=" ORDER BY rowid DESC LIMIT %i"%nlast # must be int

        with con:    
            cur = con.cursor()    

            cprint("SELECT * FROM cacheindex"+selection_string+nlast_string)

            t0=time.time()
            self.retry_execute(cur,"SELECT * FROM cacheindex"+selection_string+nlast_string)
            rows = cur.fetchall()
            cprint("mysql request took",time.time()-t0,"{log:top}")


            cprint("found rows",len(rows))
            for h,c in rows:
                try:
                    c=cPickle.loads(str(c))
                    cprint(str(h),str(c))
                except Exception as e:
                    cprint("exception while loading:",e)
                    raise

        return len(rows)

    def retry_execute(self,cur,*a,**aa):
        timeout=a['timeout'] if 'timeout' in aa else 10
        for x in range(timeout):
            try:
                return cur.execute(*a)
            except Exception as e:
                cprint(render("{RED}sqlite execute failed, try again{/}: "+repr(e)),x)
                time.sleep(1)
        raise e

    def find(self,hashe):

        con=self.connect()

        cprint("requested to find",hashe)

        with con:    
            cur = con.cursor()    
            cprint("now rows",cur.rowcount)

            try:
                self.retry_execute(cur,"SELECT content FROM cacheindex WHERE hashe=?",(self.hashe2signature(hashe),))
            except Exception as e:
                cprint("failed:",e)
                return None
            #cur.execute("SELECT content FROM cacheindex WHERE hashe=?",(self.hashe2signature(hashe),))
            try:
                rows = cur.fetchall()
            except Exception as e:
                cprint("exception while fetching",e)
                return None

        if len(rows)==0:
            cprint("found no cache")
            return None
        
        if len(rows)>1:
            cprint("multiple entries for same cache!")
            #raise Exception("confused cache! mupltile entries! : "+str(rows))
            print ("confused cache! mupltile entries! : "+str(rows),"{log:reflections}")
            print ("confused cache will run it again","{log:reflections}")
            return None

        return cPickle.loads(str(rows[0][0]))


    def make_record(self,hashe,content):

        cprint("will store",hashe,content)

        #con = lite.connect(self.filecacheroot+'/index.db')
        con=self.connect()

        c=cPickle.dumps(content)
        cprint("content as",c)

        with con:
            cur = con.cursor()    
            self.retry_execute(cur,"CREATE TABLE IF NOT EXISTS cacheindex(hashe TEXT, content TEXT)")
            self.retry_execute(cur,"INSERT INTO cacheindex VALUES(?,?)",(self.hashe2signature(hashe),c))

            cprint("now rows",cur.rowcount)

    def load_content(self,hashe,c):
        cprint("restoring from sqlite")
        cprint("content",c['content'])
        return c['content']

#import MySQLdb

class CacheMySQL(CacheSqlite):
    cache={}
                
# also to object
    total_attempts=0
    failed_attempts=0

    def statistics(self):
        if self.con is None:
            cprint("NOT connected")
        else:
            cprint("connected to",self.con)
        cprint("operations total/failed",self.total_attempts,self.failed_attempts)

    def connect(self):
        raise Exception("mysql disabled")
        if self.db is None:
            cprint("connecting to mysql")
            self.db = MySQLdb.connect(host="apcclwn12", # your host, usually localhost
                      user="root", # your username
                      port=42512,
                      #unix_socket="/workdir/savchenk/mysql/var/mysql.socket",
                      passwd=open(os.environ['HOME']+"/.secret_mysql_password").read().strip(), # your password
                      db="ddacache") # name of the data base

        return self.db

    def __init__(self,*a,**aa):
        cprint(a,aa)
        super(CacheMySQL, self).__init__(*a, **aa)
        self.db=None
        #self.connect()

    def list(self,select=None,nlast=None):

        con=self.connect()
        cprint("listing cache")

        selection_string=""
        if select is not None:
            selection_string=" WHERE "+select # must be string

        nlast_string=""
        if nlast is not None:
            nlast_string=" ORDER BY rowid DESC LIMIT %i"%nlast # must be int

        with con:    
            cur = con.cursor()

            cprint("SELECT * FROM cacheindex"+selection_string+nlast_string)

            self.retry_execute(cur,"SELECT * FROM cacheindex"+selection_string+nlast_string)
            rows = cur.fetchall()

            cprint("found rows",len(rows))
            for h,fh,c in rows:
                try:
                    c=cPickle.loads(str(c))
                    cprint(str(h),str(c))
                except Exception as e:
                    cprint("exception while loading:",e)
                    raise

        return len(rows)

    def retry_execute(self,cur,*a,**aa):
        timeout=a['timeout'] if 'timeout' in aa else 10
        for x in range(timeout):
            try:
                cprint(a)
                self.total_attempts+=1
                return cur.execute(*a)
            except Exception as e:
                self.failed_attempts+=1
                cprint(render("{RED}mysql execute failed, try again{/}: "+repr(e)),x)
                time.sleep(1)
        raise e

    def find(self,hashe):

        cprint("requested to find",hashe)
        cprint("hashed",hashe,"as",self.hashe2signature(hashe))

        db=self.connect()

        if True:    
            cur = db.cursor()    
            cprint("now rows",cur.rowcount)

            try:
                t0=time.time()
                self.retry_execute(cur,"SELECT content FROM cacheindex WHERE hashe=%s",(self.hashe2signature(hashe),))
                cprint("mysql request took",time.time()-t0,"{log:top}")
            except Exception as e:
                cprint("failed:",e)
                return None
            #cur.execute("SELECT content FROM cacheindex WHERE hashe=?",(self.hashe2signature(hashe),))
            rows = cur.fetchall()

        if len(rows)==0:
            cprint("found no cache")
            return None
        
        if len(rows)>1:
            cprint("multiple entries for same cache!")
            cprint(rows)
            return None
            #raise Exception("confused cache! mupltile entries!")

        return cPickle.loads(str(rows[0][0]))


    def make_record(self,hashe,content):
        import json

        cprint("will store",hashe,content)

        #con = lite.connect(self.filecacheroot+'/index.db')
        db=self.connect()

        c=cPickle.dumps(content)
        cprint("content as",c)

        if "_da_cached_path" in content:
            aux1=content['_da_cached_path']
        else:
            aux1=""

        with db:
            cur = db.cursor()    
            self.retry_execute(cur,"CREATE TABLE IF NOT EXISTS cacheindex(hashe TEXT, fullhashe TEXT, content TEXT)")
            self.retry_execute(cur,"INSERT INTO cacheindex (hashe,fullhashe,content,timestamp,refdir) VALUES(%s,%s,%s,%s,%s)",(self.hashe2signature(hashe),json.dumps(hashe),c,time.time(),aux1))

            cprint("now rows",cur.rowcount)

    def load_content(self,hashe,c):
        cprint("restoring from sqlite")
        cprint("content",c['content'])
        return c['content']
    
    def make_delegation_record(self,hashe,module_description,dependencies):
        import json

        cprint("will store",hashe,module_description)

        #con = lite.connect(self.filecacheroot+'/index.db')
        db=self.connect()

        shorthashe=self.hashe2signature(hashe)

        if dependencies is not None and dependencies!=[]: # two??..
            status="waiting for:"+",".join(dependencies) # comas?
        else: 
            status="ready to run"

        with db:
            cur = db.cursor()    
            self.retry_execute(cur,"CREATE TABLE IF NOT EXISTS delegationindex(id MEDIUMINT NOT NULL AUTO_INCREMENT, timestamp DOUBLE, hashe TEXT, fullhashe TEXT, modules TEXT, status TEXT, PRIMARY KEY (id))")
            self.retry_execute(cur,"INSERT INTO delegationindex (timestamp,hashe,fullhashe,modules,status) VALUES(%s,%s,%s,%s,%s)",(time.time(),shorthashe,json.dumps(hashe),json.dumps(module_description),status))

            cprint("now rows",cur.rowcount)

        return shorthashe



class TransientCache(Cache): #d
    # currently store each cache in a file; this is not neccesary 
    #__metaclass__ = decorate_all_methods
    cache={}

    parent=None

    def load(self):
        pass
    
    def save(self):
        pass

    def __repr__(self):
        return "[TransientCache of size %i at %s]"%(len(self.cache.keys()),str(id(self)))

    def list(self):
        for a,b in self.cache.items():
            cprint(a,":",b)

    def reset(self):
        self.cache={}
        cprint("resetting cache",self,"was",self.cache)

    def restore(self,hashe,obj,rc=None):
        #return # problem with files

        if obj.run_for_hashe or obj.mutating:
            return 
        # check if updated

        self.list()

        c=self.find(hashe)
        if c is None:
            if not obj.cached:
                cprint("object is not cached, i.e. only transient level cache; not leading to parent")
                return
            return self.restore_from_parent(hashe,obj,rc)

        if hasattr(c,'_da_recovered_restore_config') and c._da_recovered_restore_config!=rc:
            cprint("object in Transient cache was recovered with a different restore config: need to restore from parent")
            return self.restore_from_parent(hashe,obj,rc)

        cprint("transient cache stores results in the memory, found:",c)


        obj.import_data(c)
        
        cprint("also files restores are ignored")

        cprint("restored")
        return True

    def store_to_parent(self,hashe,obj):
        return
        if not obj.cached:
            cprint("object is not cached, i.e. only transient level cache; not leading to parent")
            return

        #if self.parent is None:
            cprint("no parent to push up to")
         #   return

        #cprint("parent to push up to:",self.parent)
        #self.parent.store(hashe,obj)

    def store(self,hashe,obj):
       # return # problem with files

        cprint("storing in memory cache:",hashe)
        if obj.run_for_hashe or obj.mutating:
            return 
        if self.readonly_cache:
            return self.store_to_parent(hashe,obj)

        obj._da_stamp=obj.get_stamp() # or in the object?
        
        if not hasattr(self,'cache'):
            self.cache={}

        content=obj.export_data()

        self.cache[hashe]=content

        cprint("stored")
        #self.list()

#        self.guarantee_unique_names(obj)
        
 #   def guarantee_unique_names(self,obj):
  #      pass

        #self.store_to_parent(hashe,obj)

class CacheIndex(Cache):
    def __init__(self,*a,**aa):
       # cprint(a,aa)
        print("vvvvv:")
        super(CacheIndex, self).__init__(*a, **aa)

    def find(self,hashe):

        cprint("requested to find",hashe)

        cached_path=self.construct_cached_file_path(hashe,None)

        if os.path.exists(cached_path+"/cache.pickle.gz"):
            cprint("found cache file:",cached_path+"/cache.pickle.gz")
            try:
                return self.load_content(hashe,None)
            except Exception as e:
                cprint("failed to load content! :"+repr(e))
                return None

        cprint("no file found in",cached_path)
        return None

    def make_record(self,hashe,content):
        #raise Exception("please write to index!")
        return

class CacheNoIndex(Cache):
    def __init__(self,*a,**aa):
        cprint(a,aa)
        super(CacheNoIndex, self).__init__(*a, **aa)

    def find(self,hashe):

        cprint("requested to find",hashe)

        cached_path=self.construct_cached_file_path(hashe,None)
        if self.filebackend.exists(cached_path+"/cache.pickle.gz"):
            cprint("found cache file:",cached_path+"/cache.pickle.gz")
            try:
                return self.load_content(hashe,None)
            except Exception as e:
                cprint("faild to load content! :"+repr(e))
                return None

        cprint("no file found in",cached_path)
        return None

    def make_record(self,hashe,content):
        #raise Exception("please write to index!")
        return


class MemCacheIRODS(CacheNoIndex):
    can_url_to_cache=False
    filebackend=caches.backends.IRODSFileBackend()

class MemCacheSSH(CacheNoIndex):
    can_url_to_cache=False
    filebackend=caches.backends.SSHFileBackend()


class CacheModule(Cache):
    filecacheroot=os.environ['DDA_MODULE_CACHE'] if 'DDA_MODULE_CACHE' in os.environ else ""

    def construct_cached_file_path(self,hashe,obj):
        print("requested path for",hashe,obj)

        def hash_to_path(hashe):
            if isinstance(hashe,tuple):
                if hashe[0]=="analysis": # more universaly
                    return hash_to_path(hashe[2])+"/"+hash_to_path(hashe[1])
                if hashe[0]=="list": # more universaly
                    return "..".join(map(hash_to_path,hashe[1:]))
                raise Exception("unknown tuple in the hash:"+str(hashe))
            if isinstance(hashe,str):
                return hashe
            raise Exception("unknown class in the hash:"+str(hashe))

        def hash_to_path2(hashe):
            #by32=lambda x:x[:8]+"/"+by8(x[8:]) if x[8:]!="" else x
            return hashe[2]+"/" + hashtools.shhash(repr(hashe[1]))

        return self.filecacheroot+"/"+hashe[1][1]+"/"+hashe[1][2]+"/" # choose to avoid overlapp

class CacheModuleIRODS(CacheNoIndex):
    filecacheroot=os.environ['DDA_MODULE_CACHE_IRODS'] if 'DDA_MODULE_CACHE_IRODS' in os.environ else ""
    filebackend=caches.backends.IRODSFileBackend()

    def construct_cached_file_path(self,hashe,obj):
        print("requested path for",hashe,obj)

        def hash_to_path(hashe):
            if isinstance(hashe,tuple):
                if hashe[0]=="analysis": # more universaly
                    return hash_to_path(hashe[2])+"/"+hash_to_path(hashe[1])
                if hashe[0]=="list": # more universaly
                    return "..".join(map(hash_to_path,hashe[1:]))
                raise Exception("unknown tuple in the hash:"+str(hashe))
            if isinstance(hashe,str):
                return hashe
            raise Exception("unknown class in the hash:"+str(hashe))

        def hash_to_path2(hashe):
            #by32=lambda x:x[:8]+"/"+by8(x[8:]) if x[8:]!="" else x
            return hashe[2]+"/" + hashtools.shhash(repr(hashe[1]))

        return self.filecacheroot+"/"+hashe[1][1]+"/"+hashe[1][2]+"/" # choose to avoid overlapp
    
