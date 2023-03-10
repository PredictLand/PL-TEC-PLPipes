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
    assert c["foo.doz.d"] == 6

def test_list():
    assert c["list"] == [1, 9, "foo"]

def test_specificity():
    assert c["foo.doz.f"] == "f"

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
