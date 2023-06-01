
import sys
import os
from pathlib import Path

sys.path.append(str(Path(os.getcwd()).joinpath("src")))

import plpipes.config

from plpipes.config import _merge_any, ConfigStack, _Ptr

import yaml

import unittest

text="""
foo:
  number: 7
  a: a
  b: True
  c: Null
  bar:
    d: d
  doz:
    e: e
    f: f
  '*':
    f: 'f*'

'*':
  doz:
    d: 6

list:
  - 1
  - 9
  - foo

"""

def mk_cfg():
    cfg = plpipes.config.ConfigStack().root()
    tree = yaml.safe_load(text)
    cfg.merge(tree)
    return cfg

c = mk_cfg()

def test_number():
    assert c["foo.number"] == 7

def test_str():
    assert c["foo.a"] == "a"

def test_wildcard():
    assert c["foo.bar.f"] == "f*"

def test_wildcard_2():
    assert c["foo.bar.d"] == "d"

def test_wildcard_10():
    assert c["foo.doz.d"] == 6

def test_list():
    assert c["list"] == [1, 9, "foo"]

def test_specificity():
    assert c["foo.doz.f"] == "f"

def test_specificity_2():
    assert c["foo.doz.e"] == "e"

def test_null():
    assert c["foo.c"] is None

def test_bool():
    assert c["foo.b"] is True

def test_key_error():
    try:
        c["foo.bar.g"]
    except Exception as ex:
        assert isinstance(ex, KeyError)
    else:
        assert False, "Exception missing!"

def test_value_error():
    try:
        c["foo.g"]
    except Exception as ex:
        assert isinstance(ex, ValueError)
    else:
        assert False, "Exception missing!"

def test_merge_any():
    # Test case 1: Merge dictionary into dictionary
    tree1 = {
        'a': 1,
        'b': {
            'c': 2,
            'd': {
                'e': 3
            }
        }
    }
    new1 = {
        'b': {
            'd': {
                'f': 4
            },
            'g': 5
        },
        'h': 6
    }
    expected1 = {
        'a': 1,
        'b': {
            'c': 2,
            'd': {
                'e': 3,
                'f': 4
            },
            'g': 5
        },
        'h': 6
    }

    assert _merge_any(tree1, new1) == expected1

    # Test case 2: Merge list into list
    tree2 = [1, [2, [3]]]
    new2 = [4, [5, [6]]]
    expected2 = [[1, [2, [3, 4, [5, [6]]]]]]

    # assert _merge_any(tree2, new2) == expected2

    # Test case 3: Merge dicts and lists
    tree3 = {
        'a': 1,
        'b': [2, 3]
    }
    new3 = {
        'b': [4, 5],
        'c': 'hello'
    }
    expected3 = {
        'a': 1,
        'b': [2, 3, 4, 5],
        'c': 'hello'
    }
    assert _merge_any(tree3, new3) == expected3

# Config stack tests
def test_stack_init():
    cfg_stack = ConfigStack()

    assert cfg_stack._frames == []
    assert cfg_stack._cache == {}

def test_stack_get_existing_key():
    cfg_stack = ConfigStack()
    cfg = cfg_stack.root()
    
    cfg['foo'] = 'bar'

    assert cfg_stack._get('foo') =='bar'
 

def test_stack_get_existing_key_with_frames():
    cfg_stack = ConfigStack()
    cfg_stack._frames[{'foo' : 'bar'}]

    assert cfg_stack._get('foo', frame = 0) == 'bar'
        
def test_stack_get_nonexistent_key():
    cfg_stack = ConfigStack()

    assert cfg_stack._get('nonexistent') is None

def test_stack_get_nonexistent_key_with_frame():
    cfg_stack = ConfigStack()

    assert cfg_stack._get('nonexistent', frame = 0) is None

def test_stack_key_exists():
    cfg_stack = ConfigStack()
    cfg = cfg_stack.root()

    cfg['foo'] = 'bar'

    assert cfg_stack._contains('foo') == True

def test_stack_key_does_not_exist():
    cfg_stack = ConfigStack()
    cfg = cfg_stack.root()

    assert cfg_stack._contains('foo') == False   

def test_stack_set_values():
    cfg_stack = ConfigStack()
    cfg = cfg_stack.root()

    cfg_stack._set("key1", "value1")
    assert cfg_stack._get("key1") == "value1"

    cfg_stack._set("key2", 54)
    assert cfg_stack._get("key2") == 54

    cfg_stack._set("key3", 54.4)
    assert cfg_stack._get("key3") == 54.4

    cfg_stack._set("key4", True)
    assert cfg_stack._get("key4") == True

    cfg_stack._set("key5", [1, 2, 3])
    assert cfg_stack._get("key5") == [1, 2, 3]

def test_stack_to_tree():
    
    pass

def test_stack_merge():
    pass

def test_reset_cache():
    cfg_stack = ConfigStack()
    cfg_stack.reset_cache()
    assert len(cfg_stack._cache) == 0

def test_stack_multicd():
    pass

def test_stack_to_tree():
    pass

def test_stack_keys():
    pass

def test_stack_squash_frames():
    pass

# Ptr tests
def test_ptr_merge():
    pass

def test_stack_merge_file():
    pass

def test_ptr_len():
    cfg_stack = ConfigStack()
    #cfg_stack_ptr = _Ptr(cfg_stack, )
    assert cfg_stack_ptr.__len__() == 0