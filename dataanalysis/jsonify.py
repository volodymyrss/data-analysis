

#TODO: this should be moved to astronomy-specific package and avoid late import

import numpy as np

try:
    from astropy.io import fits as pyfits
except ImportError:
    print("WARNING: no astropy, disabling fits jsonification")
    print("ERROR: and failing, for not")
    raise



def jsonify(item):
    from dataanalysis import core as da # late import
    if isinstance(item, da.DataFile):
        return ('DataFile', item.path, item.jsonify())

    if isinstance(item, np.ndarray):
        return jsonify_array(item)
    

    return totype(item)

def jsonify_image(data):
    if data is None:
        return None
    return [jsonify_image(d) if isinstance(d, np.array) else d for d in data]


def totype(v):
    if isinstance(v, str): return v
    if isinstance(v, int): return v
    if isinstance(v, float): return v

    if hasattr(v, 'dtype'):
        if v.dtype in (np.int16, np.int32, np.int64):
            return int(v)
        if v.dtype in (np.float16, np.float32, np.int64):
            return float(v)

    if isinstance(v, dict):
        return dict([[a, totype(b)] for a, b in list(v.items())])

    if isinstance(v, list):
        return [totype(b) for b in v]

    if isinstance(v, tuple):
        return tuple([totype(b) for b in v])
    
    if isinstance(v, Exception):
        if str(v)!="":
            return str(v)
        else:
            return repr(v)

    try:
        return float(v)
    except:
        return str(v)


def jsonify_fits_header(h):
    return dict([(k, str(h[k])) for k in list(h.keys())])


def jsonify_array(arr):
    return [totype(v) for v in arr]


def jsonify_fits_table(d):
    r = []
    for c in d.columns:
        try:
            arr = jsonify_array(c.array[:])
            r.append([c.name, arr])
        except:
            print((c, arr))
            raise
    return r


def jsonify_fits(file_handle):
    fits=pyfits.open(file_handle)

    if isinstance(fits, pyfits.HDUList):
        return [jsonify_fits(f) for f in fits]

    if isinstance(fits, pyfits.ImageHDU) or isinstance(fits, pyfits.PrimaryHDU):
        return (jsonify_fits_header(fits.header), jsonify_image(fits.data))

    if isinstance(fits, pyfits.TableHDU) or isinstance(fits, pyfits.BinTableHDU):
        return (jsonify_fits_header(fits.header), jsonify_fits_table(fits.data))

    return str(fits)
