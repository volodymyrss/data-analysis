

def test_nested_flattener():
    from dataanalysis.core import flatten_nested_structure

    assert flatten_nested_structure({},lambda x,p:x)==[]

    print((flatten_nested_structure({'a':1}, lambda p,x: (p,x))))
    print((flatten_nested_structure({'a': 1, 'b':[1,2]}, lambda p, x: (p, x))))
    print((flatten_nested_structure({'a': 1, 'b': [1, 2]}, lambda p, x: (p, x))))
    print((flatten_nested_structure({'a': 1, 'b': {'c':[1, 2]}}, lambda p, x: (p, x))))

    s={'a': 1, 'b': {'c': [1, 2],'d':''}}

    def f(k, p):
        if p==1:
            return k,p*10
        if isinstance(p,str):
            return k,".".join(k)
        return k,p

    print((flatten_nested_structure(s, f)))

def test_nested_mapper():
    from dataanalysis.core import map_nested_structure

    assert map_nested_structure({},lambda x,p:x)=={}

    print((map_nested_structure({'a':1}, lambda p,x: x)))
    print((map_nested_structure({'a': 1, 'b':[1,2]}, lambda p, x: x)))
    print((map_nested_structure({'a': 1, 'b': {'c':[1, 2]}}, lambda p, x: x)))

    s={'a': 1, 'b': {'c': [1, 2],'d':''}}

    def f(k, p):
        if p==1:
            return p*10
        if isinstance(p,str):
            return p+".".join(k)
        return p

    print((map_nested_structure(s, f)))