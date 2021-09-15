try:
    import pickle
except ImportError:
    import pickle as cPickle

import astropy.io.fits as fits # this should be specific to ddosa!

import tarfile
import traceback
import glob
import copy
import gzip
import os
import re
import pprint
import shutil
import subprocess
import time
import socket
import yaml as yaml
import random


import dataanalysis
from dataanalysis.printhook import log

log(dataanalysis.__file__)

from dataanalysis import analysisfactory
from dataanalysis import hashtools
from dataanalysis.printhook import log
from dataanalysis.caches import backends
#from dataanalysis.core import DataFile, map_nested_structure, flatten_nested_structure

global_readonly_caches=False

def is_datafile(b):
# delayed import
    from dataanalysis.core import DataFile

    return isinstance(b,DataFile)

def update_dict(a,b):
    return dict(list(a.items())+list(b.items()))


class FailedToRestoreDataFile(Exception):
    pass

class Cache(object):
    # currently store each cache in a file; this is not neccesary 
   # __metaclass__ = decorate_all_methods

    cache={}
    filecacheroot=None

    parent=None

    readonly_cache=False

    restore_adoptions=True

    ingore_unhandled=True

    filebackend=backends.FileBackend()

    can_url_to_cache=True

    def approved_hashe(self, hashe):
        return True

    def approved_read_cache(self, obj):
        if not any([isinstance(self,c) for c in obj.read_caches]):
            log("cache "+repr(self)+" should not be read by this analysis, allowed: "+repr(obj.read_caches))
            return False
        return True
    
    def approved_write_cache(self, obj):
        if not any([isinstance(self,c) for c in obj.write_caches]):
            log("cache "+repr(self)+" should not be read by this analysis, allowed: "+repr(obj.read_caches))
            return False
        return True
 
    def reset(self):
        log("resetting cache PLACEHOLDER",self)

    def reset_stack(self):
        self.reset()
        if self.parent is not None:
            self.parent.reset()

    def tail_parent(self,new_tail_parent):
        if self.parent is None:
            self.parent=new_tail_parent
        else:
            self.parent.tail_parent(new_tail_parent)

    def list_parent_stack(self):
        if self.parent is None:
            return [self]
        else:
            return [self]+self.parent.list_parent_stack()


    def statistics(self):
        pass

    def hashe2signature(self,hashe_raw):
        hashe= hashtools.hashe_replace_object(hashe_raw, None, "None")
        log("hashe:",hashe)
        if isinstance(hashe,tuple):
            if hashe[0]=="analysis":
                return hashe[2]+":" + hashtools.shhash(hashe)[:8]
        sig= hashtools.shhash(hashe)[:8]
        log("signature hashe:",sig)
        return sig

    def __init__(self,rootdir=None):
        if rootdir is not None:
            self.filecacheroot=rootdir

        if self.filecacheroot is None:
            self.filecacheroot=os.environ.get('DDA_DEFAULT_CACHE_ROOT',os.getcwd()+"/filecache")

    def __repr__(self):
        return "["+self.__class__.__name__+" of size %i at %s]"%(len(list(self.cache.keys())),self.filecacheroot)

    def find_content_hash_obj(self,hashe, obj):
        return

    def find(self,hashe):
        self.load()

        if hashe in self.cache:
            fi=self.cache[hashe]
            log("{log:cache}","cache found!",fi)
            return fi
            
        log("found no cache for",hashe,"in",self)

        return None
    
    def runtime_update(self,obj,c):
        pass
        #if self.parent is not None:
        #    self.parent.runtime_unregister(obj)

    _da_disable_parent = False
    
    def restore_from_parent(self,hashe,obj,rc=None):
        if self.parent is None:
            log("no parent available to call for in",self)
            return None
        
        log(self,"there is a parent available to call for:",self.parent)

        if not self._da_disable_parent:
            from_parent=self.parent.restore(hashe,obj,rc)
                
            if from_parent is not None:
                log("storing what restored from parent? NO!") 
                # TODO: special case when object was cached and not available in tcurrent dir
               # self.store(hashe,obj)
        else:
            log("following parent is disabled in this restore request") 
            from_parent = None

        return from_parent

    def store_to_parent(self,hashe,obj):
        if self.parent is None:
            log("no parent available to call for")
            return None
        
        log(self,"there is a parent available to call for:",self.parent)
        return self.parent.store(hashe,obj)


    def load_content(self,hashe, obj, cached_path=None):
        if cached_path is None:
            cached_path = self.construct_cached_file_path(hashe, obj)
                    
        log("load_content is restoring from",cached_path+"/cache.pickle.gz", level="top")

        try:
            log("trying loading from pickle")
            f = self.filebackend.open(cached_path+"/cache.pickle.gz",gz=True)
            try:
                content=pickle.load(f)
            except: # unicodeerror
                log("unicode compatibility with py2 caches")
                u = pickle._Unpickler(f)
                u.encoding = 'latin1'
                content = u.load()

            log("done loading from pickle")
            return content
        except TypeError as e:
            log("typeerror! "+repr(e))
            raise
        except (IOError, pickle.UnpicklingError) as e:
            log("problem loading cache! corrupt cache!", e)
            raise
        except Exception as e:
            log("problem loading cache! corrupt cache!", e)
            raise

    def restore_file(self,origin,dest,obj,hashe):
       # statistics 
        log("restore file:")
        log("< ",origin,'{log:top}',level='top')
        log("> ",dest,'{log:top}',level='top')
        
        dest_unique=dest+"."+self.hashe2signature(hashe)
        log("> ",dest_unique,'{log:top}',level='top')
        
        log("as",dest_unique,level='top')

        fsize=self.filebackend.getsize(origin)/1024./1024.
        log("restoring file of",fsize,'{log:resources}','{log:cache}')

        t0=time.time()

        if dest.endswith(".gz"):
            self.filebackend.get(origin,dest_unique,gz=False)
        else:
            self.filebackend.get(origin,dest_unique,gz=True)

        tspent=time.time()-t0

        log("restoring took",tspent,"seconds, speed",fsize/tspent,'MB/s','{log:resources}','{log:cache}')

        tspentc=0

        log("here should verify integrity")
        log("successfully restored:",dest_unique)

        if os.path.exists(dest):
            log("destination exists:",dest)

        shutil.copyfile(dest_unique,dest)

        if obj.test_files:
            log("will test file", dest, level='top')
            try:
                self.test_file(dest)
                log("test file", dest, level='top')
            except Exception as e:
                log("\033[31mtesting file", dest, "revealed issue", e,"\033[0m", level='top')
                raise
        else:
            log("\033[31mwill NOT test file\033[0m", dest, level='top')
            self.test_file(dest)
        
        log("successfully copied to",dest)

        return {'size':fsize,'copytime':tspent,'compressiontime':tspentc},dest_unique

# this should be elsewhere!
    def test_file(self,fn):
        if fn.endswith('fits') or fn.endswith('fits.gz'):
            log("fits, will test it")
            try:
                fits.open(fn)
            except Exception as e:
                log("corrupt fits file",fn,e)
                raise Exception('corrupt fits file in cache: '+fn)
        
        if fn.endswith('npy'):
            log("npy, will test it")
            import numpy
            try:
                numpy.load(fn)
            except:
                log("corrupt npy file",fn)
                raise Exception('corrupt fits file in cache: '+fn)
        
        if fn.endswith('npy.gz'):
            log("npy.gz, will test it")
            import numpy
            try:
                numpy.load(gzip.open(fn))
            except:
                log("corrupt npy.gz file",fn)
                raise Exception('corrupt fits file in cache: '+fn)

    def store_file(self,origin,dest):
        log("store file:")
        log("< ",origin,'{log:top}')
        log("> ",dest,'{log:top}')

        fsize=self.filebackend.getsize(origin)/1024./1024.
        log("storing file of",fsize,'{log:resources}','{log:cache}')

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
            log("compressing took",tspentc,"seconds, speed",fsize/tspentc,'MB/s','{log:resources}','{log:cache}')

        t0=time.time()

        self.filebackend.put(origin_gzipped,dest_gzipped) # just by name? # gzip optional
        #shutil.copyfile(origin+".gz",dest+".gz") # just by name? # gzip optional
        tspent=time.time()-t0

        log("storing took",tspent,"seconds, speed",fsize/tspent,'MB/s','{log:resources}','{log:cache}')

        return {'size':fsize,'copytime':tspent,'compressiontime':tspentc}

    def restore_datafile(self, a, b, cached_path, restore_config, obj, hashe, add_keys, remove_keys):
        log("requested to restore_datafile DataFile", b, "mode", restore_config['datafile_restore_mode'], level='top')

        prefix = restore_config.get('datafile_target_dir', None)
        if prefix is not None:
            prefix = prefix + "/"
            try:
                os.makedirs(prefix)
            except:
                pass
        else:
            prefix = ""

        try:
            if os.path.basename(b.path).endswith(".gz"):
                stored_filename = os.path.join(cached_path, os.path.basename(b.path))  # just by name? # gzip optional
            else:
                stored_filename = os.path.join(cached_path, os.path.basename(b.path) + ".gz")  # just by name? # gzip optional
            log("stored filename:", stored_filename)
        except Exception as e:
            log("wat", e)
            
        if not self.filebackend.exists(stored_filename):  # and
            log("file from cache does not exist, while cache record exists! inconsistent cache!")  # ,stored_filename)
            return None

        b.cached_path_valid_url = False
        # other way
        if restore_config['datafile_restore_mode'] == "copy":
            try:
                log('restore_config["datafile_restore_mode"] == "copy"', stored_filename)
                if not self.filebackend.exists(stored_filename):
                    log("can not copy from from cache, while cache record exists! inconsistent cache!", stored_filename,
                        level='top')
                    # raise Exception("can not copy from from cache, while cache record exists! inconsistent cache!")
                    # just reproduce?
                    return None

                log("stored file:", stored_filename, "will restore in prefix", prefix, "as", b.path, level='top')

                b.restore_stats, restored_file = self.restore_file(stored_filename, prefix + os.path.basename(b.path),
                                                                   obj, hashe)
                log("restored as", restored_file, level='top')

                obj.note_resource_stats({'resource_type': 'cache', 'resource_source': repr(self), 'filename': b.path,
                                         'stats': b.restore_stats, 'operation': 'restore'})
                b._da_unique_local_path = restored_file
                b.restored_path_prefix = os.getcwd() + "/" + prefix

                log("note unique file name", b._da_unique_local_path)

            except IOError:
                if IOError.errno == 20:
                    log("can not copy from from cache, while cache record exists! inconsistent cache!")
                    #          raise Exception("can not copy from from cache, while cache record exists! inconsistent cache!")
                    # just reproduce?
                    #return None
                    raise FailedToRestoreDataFile()
            except subprocess.CalledProcessError:
                log("can not copy from from cache, while cache record exists! inconsistent cache!")
                #           raise Exception("can not copy from from cache, while cache record exists! inconsistent cache!")
                # just reproduce?
                #return None
                raise FailedToRestoreDataFile()
            except Exception as e:
                log("UNHANDLED can not copy from from cache, while cache record exists! inconsistent cache!", level="top")
                log("details:" + repr(e))
                if not self.ingore_unhandled:
                    raise
                    #           raise Exception("can not copy from from cache, while cache record exists! inconsistent cache!")
                # just reproduce?
                raise FailedToRestoreDataFile()
                #return None
        elif restore_config['datafile_restore_mode'] == "symlink":
            self.filebackend.symlink(stored_filename,
                                     prefix + os.path.basename(b.path) + ".gz")  # just by name? # gzip optional
        elif restore_config['datafile_restore_mode'] == "urlfile":
            b.cached_path_valid_url = True
            open(prefix + os.path.basename(b.path) + ".url.txt", "w").write(
                stored_filename + "\n")  # just by name? # gzip optional
        elif restore_config['datafile_restore_mode'] == "urlfileappend":
            open(prefix + os.path.basename(b.path) + ".urls.txt", "a").write(
                stored_filename + "\n")  # just by name? # gzip optional
            b.cached_path_valid_url = True
        elif restore_config['datafile_restore_mode'] == "url_in_object":
            b.cached_path = stored_filename  # just by name? # gzip optional
            if not os.path.exists(b.cached_path):
                raise Exception("cached file does not exist!")
            b.cached_path_valid_url = True
            log("stored url:", b.cached_path, b.cached_path_valid_url, level='top')

            if 'test_files' in restore_config and restore_config['test_files']:
                try:
                    self.test_file(b.cached_path)
                except:
                    return
        else:
            raise Exception("datafile restore mode not understood!")

        b.cached_path = stored_filename
        b.restored_mode = restore_config['datafile_restore_mode']

        if hasattr(b, 'adopted_format'):
            log("found adopted format in",a,b,"old name",b.pre_adoption_key)
            if self.restore_adoptions:
                log("restoring adoption")
                add_keys.append([b.pre_adoption_key,b.restore_adoption()])
            else:
                log("NOT restoring adoption")

    def get_restore_config(self,obj,extra_restore_config=None):
        restore_config = dict(datafile_restore_mode="copy", datafile_target_dir=None)  # no string

        if extra_restore_config is not None:
            restore_config = dict(list(restore_config.items()) + list(extra_restore_config.items()))

        if obj.datafile_restore_mode is not None:
            restore_config['datafile_restore_mode']=obj.datafile_restore_mode

        return restore_config


    #####
    def restore(self, hashe, obj, restore_config=None):
        # check if updated
        if obj.run_for_hashe or obj.mutating:
            return 

        if not self.approved_read_cache(obj):
            log("cache", self, "not approved_read_cache, will restore_from_parent", level="top")
            from_parent=self.restore_from_parent(hashe,obj,restore_config)
            return from_parent

        restore_config=self.get_restore_config(obj, extra_restore_config=restore_config)
        log("will restore",self,obj,"restore_config",restore_config)

        c=self.find_content_hash_obj(hashe,obj)
        if c is None:
            log("cache",self,"found no hashe-obj record")
            c=self.find(hashe)
        else:
            log("cache", self, "found hashe-obj record")

        if c is None:
            log("restore failed: passing to parent",self.parent)
            log("cache", self, "did not find, will restore_from_parent", level="top")
            return self.restore_from_parent(hashe, obj, restore_config)

        log("requested to restore cache")
        cached_path=self.construct_cached_file_path(hashe,obj)
        log(self, "in restore, cached path:",cached_path)

        return self.restore_from_dir(cached_path, hashe, obj, restore_config)


    def restore_from_dir(self, cached_path, hashe, obj, restore_config):
        obj._da_cache_path_root=cached_path
        obj._da_cached_pathes=[cached_path]

        log("will restore object from dir", cached_path, obj, level="top")

        try:
            c = self.load_content(hashe, obj, cached_path) # why do we load it twice?..
            log("load content returns:",)
        except Exception as e:
            log("can not load content from cache, while cache record exists! inconsistent cache!") #???
            #raise Exception("can not copy from from cache, while cache record exists! inconsistent cache!") # ???
            return self.restore_from_parent(hashe,obj,restore_config)

        if not self.can_url_to_cache:
            log("cache can not be url, will copy all output",level="cache")
            restore_config['datafile_restore_mode']="copy"

        if obj.only_verify_cache_record:
            log("established cache record: will not acutally restore",level="cache")
            return True

        if not isinstance(c, dict):
            raise Exception("content from cache is not dict! "+str(c))
        else:
            add_keys=[]
            remove_keys=[]


            def datafile_restore_mapper(k, b):
                log("datafile_restore_mapper processing structure entry",k,b)
                if is_datafile(b):
                    if len(k)==1:
                        a=k[0]
                    else:
                        a=k

                    self.restore_datafile(a, b, cached_path, restore_config, obj, hashe, add_keys, remove_keys)
                return b


            from dataanalysis.core import DataFile, map_nested_structure, flatten_nested_structure
                
            log("will map_nested_structure")

            try:
                map_nested_structure(c, datafile_restore_mapper)
            except FailedToRestoreDataFile as e:
                log("\033[31mdatafile_restore_mapper failed, cache is invalid: ", e,"\033m")
                return


            for k, i in add_keys:
                log("adding key:",k,i,level=__name__)

                sub_c=c
                for ck in k[:-1]:
                    sub_c=sub_c[ck]
                sub_c[k[-1]]=i

            #for k in remove_keys:
            #    print("removing key:", k, i)
            #    del c[k]

            for k,i in list(c.items()):
                log("setting",obj,k,i,level=__name__)

                try:
                    setattr(obj,k,i)
                except Exception as e:
                    log("can not set: assuming blueprint class upgrade (might be incomplete update!)",level="core")


            obj._da_recovered_restore_config=copy.copy(restore_config)

            log("restored with",obj._da_recovered_restore_config)
    
            try:
                a=obj.verify_content()
                log("verify_content result", a)
                if a is None:
                    raise Exception("returned none")
                log("verify_content result is good")
            except Exception as e:
                log("verify_content failed",e)
                return

            log(self,"restore returning True")
            return True

    # TODO: clear the directory!
    def assemble_blob(self,hashe,obj):
        blob_dir = "./blob-{}".format(self.hashe2signature(hashe))
        self.store_to_directory(hashe, obj, blob_dir)

        with tarfile.open("tmp.tgz", "w:gz") as tar:
            for name in glob.glob(os.path.join(blob_dir, "*")):
                tar.add(name)

        tar.close()
        return open("tmp.tgz", "rb")
    
    def restore_from_blob(self, blob_fn, hashe, obj, restore_config):
        restored_dir = f"restored-blob-{time.time()}-{random.randint(1, int(1e10)):10d}"

        if os.path.exists(restored_dir):
            raise RuntimeError("temp restore dir exists before creating, impossible: {restored_dir}!")

        with tarfile.open(blob_fn, "r:gz") as tar:
            tar.extractall(restored_dir)

        return self.restore_from_dir(restored_dir+"/blob", hashe, obj, restore_config)


    def adopt_datafiles(self,content):

        extra_content={}
        remove_keys=[]

        def mapping_adoption(k, b):
            a = re.sub("[^a-zA-Z0-9\-]", "_", "_".join(map(str,k)))

            adopted_b=DataFile.from_object(k,b,optional=True)
            if adopted_b is not b:
                log("storing adopted DataFile",a,adopted_b,level="main")
                extra_content["_datafile_"+a]=adopted_b
                return None # datafile is put elsewhere

            return adopted_b

        from dataanalysis.core import DataFile, map_nested_structure, flatten_nested_structure
        content = map_nested_structure(content,mapping_adoption)

        if len(extra_content)>0:
            log("extra content:",extra_content)

        content=dict(list(content.items()) + list(extra_content.items()))

        log("after adoption, keys",list(content.keys()))

        return content


    def store_to_directory(self, hashe, obj, cached_path):
        if not cached_path.endswith("/"):
            cached_path=cached_path+"/"

        obj._da_cached_path = cached_path

        if not hasattr(obj, '_da_cached_pathes'):
            obj._da_cached_pathes = []
        obj._da_cached_pathes.append(cached_path)

        log("storing in", cached_path)


        dn = os.path.dirname(cached_path)
        if not self.filebackend.exists(dn):
            self.filebackend.makedirs(dn)

        content = obj.export_data()

        content=self.adopt_datafiles(content)

        try:
            pickle.dump(content, self.filebackend.open(cached_path + "cache.pickle.gz", "w", gz=True))
            pickle.dump(hashe, self.filebackend.open(cached_path + "hash.pickle.gz", "w", gz=True))
        except pickle.PicklingError as pe:
            log("pickling issue",pe)
            log("was pickling content",content)
            raise

        if hasattr(obj,'store_preview_yaml') and obj.store_preview_yaml:
            yamlfn=cached_path + "cache_preview.yaml.gz"
            log("storing preview yaml to",yamlfn)

            jsonified=self.adopt_datafiles(obj.jsonify())

            log("to dump preview yaml",jsonified)

            yaml.dump(
                jsonified,
                self.filebackend.open(yamlfn,"w",gz=False),
                default_flow_style=False
            )


        self.filebackend.open(cached_path + "hash.txt", "wt").write(pprint.pformat(hashe) + "\n")
        self.filebackend.open(cached_path + "log.txt.gz", "wt", gz=True).write(obj._da_main_log_content)
        
        yaml.dump(obj.get_identity().serialize(), self.filebackend.open(cached_path + "object_identity.yaml.gz","wt",gz=True),default_flow_style=False)

        aliases = obj.factory.list_relevant_aliases(obj)

        try:
            if aliases != []:
                open(cached_path + "aliases.txt", "w").write("\n".join(
                    [("=" * 80) + "\n" + pprint.pformat(a) + "\n" + ("-" * 80) + "\n" + pprint.pformat(b) + "\n" for
                     a, b in aliases]))
        except:
            pass  # fix!

        definitions = analysisfactory.AnalysisFactory.get_definitions()
        try:
            if definitions != []:
                open(cached_path + "definitions.txt", "w").write("\n".join(
                    [("=" * 80) + "\n" + pprint.pformat(a) + "\n" + ("-" * 80) + "\n" + pprint.pformat(b) + "\n" for
                     a, b in definitions]))
        except:
            pass  # fix!

        modules = obj.factory.get_module_description()
        filtered_modules = []
        for m in reversed(modules):
            if m not in filtered_modules:
                filtered_modules.append(m)
        self.filebackend.open(cached_path + "modules.txt", "w").write(pprint.pformat(filtered_modules) + "\n")

        ftext=""
        for factorization in obj.factory.factorizations:
            ftext+=("=" * 80)+"\n"
            for k,v in list(factorization.items()):
                ftext += "|" + k + "\n"
                ftext += ("-" * 80) + "\n"
                ftext += pprint.pformat(v)+"\n\n"

        self.filebackend.open(cached_path + "factorizations.txt", "w").write(ftext)

        self.filebackend.flush()

        if hasattr(obj, 'alias'):
            log('object has alias:', obj.alias)
            open(cached_path + "alias.txt", "w").write(pprint.pformat(obj.alias) + "\n")
        else:
            log('object has no alias')


        if isinstance(content, dict):
            #for a, b in content.items():
            def datafile_mapper(k,b):
                if is_datafile(b):
                    log("requested to store DataFile", b)

                    try:
                        p = cached_path + os.path.basename(b.path)
                    except Exception as e:
                        log("failed:", e)
                        log("path:", b.path)
                        log("b:", b)
                        raise
                    b.cached_path = p + ".gz" if not p.endswith(".gz") else p
                    b.store_stats = self.store_file(b.path, p)
                    b._da_cached_path = cached_path
                    b.cached_path_valid_url = True
                    obj.note_resource_stats(
                        {'resource_type': 'cache', 'resource_source': repr(self), 'filename': b.path,
                         'stats': b.store_stats, 'operation': 'store'})
                return b

            from dataanalysis.core import DataFile, map_nested_structure, flatten_nested_structure
            mapped=map_nested_structure(content,datafile_mapper)
            log("mapped structure (%i):"%len(mapped))
            for k,v in flatten_nested_structure(mapped, lambda x,y:(x,y)):
                log("---",k,v)


        return content

    def store_object_content(self,hashe,obj):
        log(self, "store object content", obj,'{log:top}')

        if not self.filebackend.exists(self.filecacheroot):
            self.filebackend.makedirs(self.filecacheroot)

        obj._da_stamp = obj.get_stamp()  # or in the object?

        if not hasattr(self, 'cache'):
            self.cache = {}

        cached_path = self.construct_cached_file_path(hashe, obj)

        return self.store_to_directory(hashe, obj, cached_path)

    def store(self, hashe, obj):
        if obj.run_for_hashe or obj.mutating:
            return
        
        if self.readonly_cache:
            return self.store_to_parent(hashe,obj)

        if global_readonly_caches:
            raise Exception("all caches are readonly!")

        log("requested to store:",hashe)
        if not obj.cached:
            log("the object is declared as non-cached, not storing")
            return
        else:
            log("object",obj,"is cached, storing")
        
        if self.approved_write_cache(obj):
            log(self, "storing:", hashe, level="top")

            content=self.store_object_content(hashe,obj)

            log("will check if record exists",'{log:top}')
            found=self.find(hashe)
            if found is None:
                self.make_record(hashe,{'host':socket.gethostname(),'recored_at':time.time(),'content':content})
            else:
                log("these results will be ignored! (why would we do this?..)","{log:reflections}") # current behavior is to replace
                self.make_record(hashe,{'host':socket.gethostname(),'recored_at':time.time(),'content':content}) # twice same!
            
        else:
            log("cache "+repr(self)+" should not be written by this analysis, allowed: "+repr(obj.write_caches))

        return self.store_to_parent(hashe,obj)

    def make_record(self,hashe,content):
        #log("make record",hashe,content)
        self.cache[hashe]=content
        self.save()
        log("now entries",len(self.cache))

    def construct_cached_file_path(self,hashe, obj=None):
        if obj is not None:
            log(f"warning: provided obj {obj} for construct_cached_file_path, but it is not used")

        log("requested default cached file path")

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


    def save(self,target=None):
        if target is None:
            target=self.filecacheroot+"/index.pickle.gz"
        pickle.dump(self.cache,gzip.open(target,"w"))

    def load(self,target=None):
        if target is None:
            target=self.filecacheroot+"/index.pickle.gz"

        if os.path.exists(target):
            self.cache=pickle.load(gzip.open(target))
        else:
            log("file to load does not exist:",target)

    def make_delegation_record(self,hashe,module_description,dependencies):
        return self.parent.make_delegation_record(hashe,module_description,dependencies)

    def register_delegation(self,obj,hashe):
        log("requested to register delegation of",obj)
        log("hashe:",obj)

        log("modules used in dda factory:")
        
        module_description=obj.factory.get_module_description()

        dependencies=obj._da_delegated_input

        log ("will store hashe       ",hashe)
        log ("will store modules     ",module_description,dependencies)
        log ("will store dependencies",dependencies)

        return self.make_delegation_record(hashe,module_description,dependencies)
    
    def report_analysis_state(self,obj,state):
        state_dir_root=self.filecacheroot+"/state_reports/"
        state_dir=state_dir_root+"/"+obj.get_signature()
        
        try:
            os.makedirs(state_dir)
        except os.error:
            log("unable to create state dir!") # exist?

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
            log(e)
        

    def report_exception(self,obj,e):
        exception_dir_root=self.filecacheroot+"/exception_reports/"
        exception_dir=exception_dir_root+"/"+obj.get_signature()
        
        try:
            os.makedirs(exception_dir)
        except os.error:
            log("unable to create exception dir!") # exissst?

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
                log(e)
        except Exception:
            log("unable to write exception!")
            log(e)


        # check
        #for m in sys.modules:
        #    log("all modules:",m,sys.modules[m],sys.modules[m].__file__ if hasattr(sys.modules[m],'__file__') else "no file??")


        


#import MySQLdb




class TransientCache(Cache): #d
    # currently store each cache in a file; this is not neccesary 
    #__metaclass__ = decorate_all_methods
    cache={}

    parent=None

    def load(self,target=None):
        pass
    
    def save(self,target=None):
        pass

    def __repr__(self):
        return "[TransientCache of size %i at %s]"%(len(list(self.cache.keys())),str(id(self)))

    def list(self):
        for a,b in list(self.cache.items()):
            log(a,":",b)

    def reset(self):
        self.cache={}
        log("resetting cache",self,"was",self.cache)

    def restore(self,hashe,obj,restore_config=None):
        #return # problem with files

        if obj.run_for_hashe or obj.mutating:
            return

        # check if updated

        self.list()

        c=self.find(hashe)

        if c is None:
            if not obj.cached:
                log("object is not cached, i.e. only transient level cache; not leading to parent")
                return
            return self.restore_from_parent(hashe,obj,restore_config)

        if hasattr(c,'_da_recovered_restore_config') and c._da_recovered_restore_config!=restore_config:
            log("object in Transient cache was recovered with a different restore config: need to restore from parent")
            return self.restore_from_parent(hashe,obj,restore_config)

        log("transient cache stores results in the memory, found:",c)


        obj.import_data(c)
        
        log("also files restores are ignored")

        log("restored")
        return True

    def store_to_parent(self,hashe,obj):
        return

    def store(self, hashe, obj):
       # return # problem with files

        log("storing in memory cache:",hashe)
        if obj.run_for_hashe or obj.mutating:
            return 
        if self.readonly_cache:
            return self.store_to_parent(hashe,obj)

        obj._da_stamp=obj.get_stamp() # or in the object?
        
        if not hasattr(self,'cache'):
            self.cache={}

        content=obj.export_data()

        self.cache[hashe]=content

        log("stored in transient",obj,hashe)


class CacheIndex(Cache):
    def __init__(self,*a,**aa):
       # log(a,aa)
        log("vvvvv:")
        super(CacheIndex, self).__init__(*a, **aa)

    def find(self,hashe):

        log("requested to find",hashe)

        cached_path=self.construct_cached_file_path(hashe,None)

        if os.path.exists(cached_path+"/cache.pickle.gz"):
            log(self, "found cache file:",cached_path+"/cache.pickle.gz")
            try:
                return self.load_content(hashe,None)
            except Exception as e:
                log("failed to load content! :"+repr(e))
                return None

        log("no file found in",cached_path)
        return None

    def make_record(self,hashe,content):
        #raise Exception("please write to index!")
        return

class CacheNoIndex(Cache):
    def __init__(self,*a,**aa):
        log(a,aa)
        super(CacheNoIndex, self).__init__(*a, **aa)

    def find(self,hashe):

        log("requested to find",hashe)

        cached_path=self.construct_cached_file_path(hashe,None)
        if self.filebackend.exists(cached_path+"/cache.pickle.gz"):
            log(self, "found cache file:", cached_path+"/cache.pickle.gz", level='top')
            try:
                return self.load_content(hashe,None)
            except Exception as e:
                log("failed to load content! :"+repr(e), level='top')
                return None

        log("no file found in",cached_path)
        return None

    def make_record(self,hashe,content):
        #raise Exception("please write to index!")
        return

class CacheBlob(Cache):
    def deposit_blob(self, hashe, blob_stream):  # 
        raise NotImplementedError

    def retrieve_blob(self, hashe): # returns stream
        raise NotImplementedError

    def store(self, hashe, obj):
        if not self.approved_hashe(hashe) or not self.approved_write_cache(obj):
            return self.store_to_parent(hashe, obj)

        log("\033[33mtrying to store blob\033[0m", level="top")
        blob = self.assemble_blob(hashe, obj)
        blob.seek(0)

        self.deposit_blob(hashe, blob)
        log("after deposit stacked factory assumptions:", obj.factory.factory_assumptions_stacked, level="top")

    def restore(self, hashe, obj, rc=None):
        if not self.approved_hashe(hashe) or not self.approved_read_cache(obj):
            return self.restore_from_parent(hashe, obj, rc)

        print("\033[33mtrying to restore from blob\033[0m")

        try:
            blob_stream = self.retrieve_blob(hashe)
        except Exception as e:
            print("problem retrieving blob:", e, "returning None")
            return
        
        if blob_stream is not None:
            blob = blob_stream.read()
            print("\033[33mrestored blob\033[0m: blob of", len(blob)/1024/1024, "kb")

            cached_blob_fn = f"restored-blob-{random.randint(1, int(1e10)):10d}.tgz"

            open(cached_blob_fn, "wb").write(blob)

            return self.restore_from_blob(cached_blob_fn, hashe, obj, rc)

class MemCacheIRODS(CacheNoIndex):
    can_url_to_cache=False
    filebackend=backends.IRODSFileBackend()

class MemCacheSSH(CacheNoIndex):
    can_url_to_cache=False
    filebackend=backends.SSHFileBackend()


class CacheModule(Cache):
    filecacheroot=os.environ['DDA_MODULE_CACHE'] if 'DDA_MODULE_CACHE' in os.environ else ""

    def construct_cached_file_path(self,hashe,obj):
        log("requested path for",hashe,obj)

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
    filebackend=backends.IRODSFileBackend()

    def construct_cached_file_path(self,hashe,obj):
        log("requested path for",hashe,obj)

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
    
