
from plpipes.config import cfg

PLURALS = cfg.to_tree("plpipes-util.pluralsingular.plurals")
SINGULARS = {lang: {v: k for k, v in plurals.items()} for lang, plurals in PLURALS.items()}

LANG = cfg.get("plpipes-util.pluralsingular.lang", "en")

def pluralize(word, lang=LANG, marks=True, plurals=None):
    if plurals is None:
        plurals = PLURALS.get(lang, {})
    else:
        plurals = {**PLURALS.get(lang, {}), **plurals}
    if isinstance(words, list):
        return [_pluralize_word(w, lang, marks, plurals) for w in words]
    else:
        return _pluralize_word(words, lang, marks, plurals)

def _pluralize_word(word, lang, marks, plurals):
    if word in plurals:
        p = plurals[word]
    else:
        import pluralsingular
        p = pluralsingular.pluralize(word, lang=lang)
    if not marks:
        import unidecode
        p = unidecode.unidecode(p)
    return p

def singularize(words, lang=LANG, marks=True, singulars=None):
    if singulars is None:
        singulars = SINGULARS.get(lang, {})
    else:
        singulars = {**SINGULARS.get(lang, {}), **singulars}
    if isinstance(words, list):
        return [_singularize_word(w, lang, marks, singulars) for w in words]
    else:
        return _singularize_word(words, lang, marks, singulars)

def _singularize_word(word, lang, marks, singulars):
    if word in singulars:
        s = singulars[word]
    else:
        import pluralsingular
        s = pluralsingular.singularize(word, lang=lang)
    if not marks:
        import unidecode
        s = unidecode.unidecode(s)
    return s
