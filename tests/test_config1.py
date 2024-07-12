import pytest

import plpipes.config
import yaml

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
@pytest.fixture(scope='module')
def cfg():
    cfg = plpipes.config.ConfigStack().root()
    tree = yaml.safe_load(text)
    cfg.merge(tree)
    return cfg

def test_number(cfg):
    assert cfg["foo.number"] == 7

def test_str(cfg):
    assert cfg["foo.a"] == "a"

def test_wildcard(cfg):
    assert cfg["foo.bar.f"] == "f*"

def test_wildcard_2(cfg):
    assert cfg["foo.doz.d"] == 6

def test_list(cfg):
    assert cfg["list"] == [1, 9, "foo"]

def test_specificity(cfg):
    assert cfg["foo.doz.f"] == "f"

def test_null(cfg):
    assert cfg["foo.c"] is None

def test_bool(cfg):
    assert cfg["foo.b"] is True

def test_key_error(cfg):
    try:
        cfg["foo.bar.g"]
    except Exception as ex:
        assert isinstance(ex, KeyError)
    else:
        assert False, "Exception missing!"

def test_value_error(cfg):
    try:
        cfg["foo.g"]
    except Exception as ex:
        assert isinstance(ex, ValueError)
    else:
        assert False, "Exception missing!"
