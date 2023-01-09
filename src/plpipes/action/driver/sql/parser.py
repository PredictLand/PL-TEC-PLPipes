from parsy import from_enum, regex, seq, string

from dataclasses import dataclass
from typing import List, Optional, Union

import enum

@dataclass
class Token:
    token: str

word=regex("[a-zA-Z_]+")
number=regex("[0-9]+")
space=regex(r"\s+")
symbol=regex(r"[!#$%&()*+-\./:<=>?\[\\\]^{|}~]+")
semicolon=string(";")
single_quote=regex(r"'")
double_quote=string('"')
quote = single_quote | double_quote


sentence = seq((word|number|space|symbol|single_quote).map(Token).many(), semicolon)


