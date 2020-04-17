import hashlib
from hashlib import sha224
from dataanalysis.printhook import log
import json

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
    except Exception as e:
        log("error while hashe_replace_object",e,level="hashe")

    try:
        x=hashe_map(x,str)
    except Exception as e:
        log("error while hash_map",e,level="hashe")

    return sha224(str(x).encode('utf-8')).hexdigest()


def hashe_map(hashe,f):
    if isinstance(hashe,tuple):
        if hashe[0]=='analysis':
            return ('analysis',hashe_map(hashe[1],f),hashe_map(hashe[2],f))
        if hashe[0]=='list':
            return ('list',)+tuple([hashe_map(h,f) for h in hashe[1:]])
        raise Exception("in hashe: \""+str(hashe)+"\" incomprehenisve entry!")
    return f(hashe)

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

def remove_repeating_stacks(input_stack):
    exclude_mask=[False]*len(input_stack)
    for stack_length in range(1,int(len(input_stack)/2)):
        for stack_start in range(0,len(input_stack)-stack_length):
            if input_stack[stack_start:stack_start+stack_length] == input_stack[stack_start+stack_length:stack_start+stack_length+stack_length]:
                log("found repetition of ",stack_start,stack_length,":",input_stack[stack_start:stack_start+stack_length*2],level="top")
                for i in range(stack_start+stack_length,stack_start+stack_length+stack_length):
                    exclude_mask[i]=True
    if sum(exclude_mask)>0:
        log("excluding",sum(exclude_mask),"out of",len(exclude_mask),level="top")
    return [inp for inp,m in zip(input_stack,exclude_mask) if not m]
