"""Environment-variable interpolation for step commands.

Commands may reference variables as ``$NAME`` or ``${NAME}``, with an
optional default: ``${NAME:-fallback}``. Expansion happens before the
command reaches the shell, against the merged environment (process
environment plus flow-level and step-level ``env``). A variable that
is unset and has no default expands to the empty string, matching
POSIX shell behavior. ``$$`` and ``$`` followed by a non-name
character are left untouched for the shell.

Syntax is validated statically - an unclosed ``${`` or an empty
``${}`` is a flow-definition error, not a runtime surprise.
"""

from __future__ import annotations

import re

_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")


def check_syntax(text: str) -> None:
    """Raise ValueError on malformed interpolation syntax in ``text``."""
    i = 0
    while i < len(text):
        if text[i] != "$":
            i += 1
            continue
        if text.startswith("$$", i):
            i += 2
            continue
        if text.startswith("${", i):
            end = text.find("}", i + 2)
            if end == -1:
                raise ValueError(f"unclosed '${{' in {text!r}")
            inner = text[i + 2:end]
            name, _, default = inner.partition(":-")
            if not name:
                raise ValueError(f"empty variable name in {text!r}")
            if not _NAME.fullmatch(name):
                raise ValueError(f"bad variable name {name!r} in {text!r}")
            if ":" in name or (inner.startswith(":") and not default):
                raise ValueError(f"malformed interpolation '${{{inner}}}' in {text!r}")
            i = end + 1
            continue
        match = _NAME.match(text, i + 1)
        i = match.end() if match else i + 1


def expand(text: str, env: dict) -> str:
    """Expand ``$NAME``/``${NAME}``/``${NAME:-default}`` against ``env``.

    Unset variables without a default expand to "". Assumes
    check_syntax(text) has passed (or text is known well-formed).
    """
    out: list[str] = []
    i = 0
    while i < len(text):
        if text[i] != "$":
            out.append(text[i])
            i += 1
            continue
        if text.startswith("$$", i):
            out.append("$$")
            i += 2
            continue
        if text.startswith("${", i):
            end = text.find("}", i + 2)
            inner = text[i + 2:end]
            name, sep, default = inner.partition(":-")
            if name in env:
                out.append(env[name])
            elif sep:
                out.append(default)
            i = end + 1
            continue
        match = _NAME.match(text, i + 1)
        if match:
            name = match.group(0)
            if name in env:
                out.append(env[name])
            i = match.end()
        else:
            out.append("$")
            i += 1
    return "".join(out)


def missing(text: str, env: dict) -> list[str]:
    """Names referenced in ``text`` that are unset in ``env`` and have no default."""
    names: list[str] = []
    i = 0
    while i < len(text):
        if text[i] != "$":
            i += 1
            continue
        if text.startswith("$$", i):
            i += 2
            continue
        if text.startswith("${", i):
            end = text.find("}", i + 2)
            inner = text[i + 2:end]
            name, sep, _ = inner.partition(":-")
            if not sep and name not in env and name not in names:
                names.append(name)
            i = end + 1
            continue
        match = _NAME.match(text, i + 1)
        if match:
            name = match.group(0)
            if name not in env and name not in names:
                names.append(name)
            i = match.end()
        else:
            i += 1
    return names
