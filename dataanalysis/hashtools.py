import hashlib
from hashlib import sha224


def hash_for_file(f, block_size=2**20):
    md5 = hashlib.md5()
    while True:
        data = f.read(block_size)
        if not data:
            break
        md5.update(data)
    return shhash(md5.digest())

def shhash(x):
    try:
        x=hashe_replace_object(x,None,'None')
    except:
        pass
    return sha224(str(hash(x)).encode('utf-8')).hexdigest()

def hashe_replace_object(hashe,what,witha):
    if hashe==what:
        return witha
    if isinstance(hashe,tuple):
        if hashe[0]=='analysis':
            return ('analysis',hashe_replace_object(hashe[1],what,witha),hashe_replace_object(hashe[2],what,witha))
        if hashe[0]=='list':
            return ('list',)+tuple([hashe_replace_object(h,what,witha) for h in hashe[1:]])
        raise Exception("in hashe: \""+str(hashe)+"\" incomprehenisve tpule!")
    #if hashe==what:
    #    return witha
    return hashe

def find_object(hashe,what):
    if hashe==what:
        return True

    if isinstance(hashe,tuple):
        if hashe[0]=='analysis':
            return find_object(hashe[1],what) or find_object(hashe[2],what)
        if hashe[0]=='list':
            return any([find_object(h,what) for h in hashe[1:]])
        raise Exception("in hashe: \""+str(hashe)+"\" incomprehenisve tpule!")
    #if hashe==what:
    #    return witha
    return False

def hashe_list_objects(hashe):
    if isinstance(hashe,tuple):
        if hashe[0]=='analysis':
            return [hashe[2]]+hashe_list_objects(hashe[1])
        if hashe[0]=='list':
            l=[]
            for h in hashe[1:]:
                l+=hashe_list_objects(h)
            return l
        raise Exception("in hashe: \""+str(hashe)+"\" incomprehenisve tuple!")
    return []
