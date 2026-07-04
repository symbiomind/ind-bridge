"""
The ELIZA transformation engine — a pure, deterministic implementation of the
algorithm described in Joseph Weizenbaum, "ELIZA — A Computer Program For the
Study of Natural Language Communication Between Man And Machine",
Communications of the ACM 9(1), January 1966.

Consumed by the ``Weizenbaum`` resource plugin. The engine is stateless by
design: :func:`respond` replays the entire visible conversation to rebuild
reassembly-cycle positions and the memory stack, so the reply is a pure
function of ``(history, script)``. Give the pipeline a session and she
remembers; take it away and she forgets. The pipeline composes the
capability — the engine never holds state.

Faithful 1966 machinery: pre-substitution, sentence segmentation, keyword
ranking with a keystack, decomposition rules (``*`` wildcards and ``@synonym``
classes), reassembly rules cycled in order (the original cycles — it does not
randomise), reflection of captured groups, ``goto`` redirects, ``newkey``,
and the MEMORY mechanism (stash on the memory keyword, recall when nothing
else matches).

Modern additions, all still pure and deterministic — no AI, that is the
point: multi-word reflection (``i am`` ↔ ``you are`` before unigram swaps),
contraction expansion via pre-substitution, emoji awareness (an ``xemoji``
script keyword), salience-weighted memory recall (the most emotionally
loaded stash entry surfaces first), and depth-gated script sections —
keywords and fallback lines that only activate once the visible history is
deep enough. What a script does with that gate is the script's business.
"""

from __future__ import annotations

import re
from dataclasses import dataclass, field

__all__ = ["Script", "ScriptError", "parse_script", "respond"]


class ScriptError(ValueError):
    """Raised by :func:`parse_script` when a script is structurally unusable
    (no keywords, no xnone fallback, malformed rules)."""


# ---------------------------------------------------------------------------
# Script model
# ---------------------------------------------------------------------------

@dataclass
class _Rule:
    decomp: list[str]           # pattern tokens: literal | "*" | "@class"
    reasmb: list[str]           # reassembly templates, cycled in order
    key: str                    # owning keyword (cycle identity)
    index: int                  # rule position within the keyword (cycle identity)


@dataclass
class _Keyword:
    word: str
    rank: int
    rules: list[_Rule]
    memory: list[_Rule]         # memory (stash) rules — usually only on "my"
    depth: int = 0              # min history depth for this entry to activate


@dataclass
class _Tier:
    depth: int
    xnone: list[str]            # extra fallback lines unlocked at this depth


@dataclass
class Script:
    greeting: str
    pre: dict[str, list[str]]           # token -> replacement token list
    post: dict[str, str]                # word/bigram -> reflection
    synonyms: dict[str, list[str]]
    keywords: list[_Keyword]            # includes depth-gated entries
    tiers: list[_Tier]
    memory_salience: set[str]           # words that make a stash entry "loaded"
    memory_depth: int                   # min depth before recall may fire


_DEFAULT_GREETING = "How do you do. Please tell me your problem."


def parse_script(data: dict) -> Script:
    """Parse a raw (YAML-loaded) script dict into a :class:`Script`."""
    if not isinstance(data, dict):
        raise ScriptError("script must be a mapping")

    synonyms = {
        str(name): [str(w).lower() for w in words]
        for name, words in (data.get("synonyms") or {}).items()
    }

    def _parse_rules(entries, key: str, offset: int = 0) -> list[_Rule]:
        rules = []
        for i, entry in enumerate(entries or []):
            decomp = str(entry.get("decomp", "*")).lower().split()
            reasmb = [str(r) for r in (entry.get("reasmb") or [])]
            if not reasmb:
                raise ScriptError(f"keyword '{key}' rule {i} has no reasmb")
            rules.append(_Rule(decomp, reasmb, key, offset + i))
        return rules

    keywords: list[_Keyword] = []

    def _parse_keyword(entry: dict, default_depth: int = 0) -> _Keyword:
        word = str(entry.get("key", "")).lower()
        if not word:
            raise ScriptError("keyword entry missing 'key'")
        return _Keyword(
            word=word,
            rank=int(entry.get("rank", 0)),
            rules=_parse_rules(entry.get("rules"), word),
            memory=_parse_rules(entry.get("memory"), f"mem:{word}", offset=1000),
            depth=int(entry.get("depth", default_depth)),
        )

    def _add_keyword(entry: dict, default_depth: int = 0) -> None:
        kw = _parse_keyword(entry, default_depth)
        keywords.append(kw)
        # Aliases register the same entry under extra trigger words. Rules
        # (and therefore reassembly cycles) are shared, not copied.
        for alias in entry.get("aliases") or []:
            keywords.append(_Keyword(
                word=str(alias).lower(), rank=kw.rank,
                rules=kw.rules, memory=kw.memory, depth=kw.depth,
            ))

    for entry in data.get("keywords") or []:
        _add_keyword(entry)

    tiers: list[_Tier] = []
    for tier in data.get("awakening") or []:
        depth = int(tier.get("depth", 0))
        tiers.append(_Tier(depth=depth, xnone=[str(x) for x in tier.get("xnone") or []]))
        for entry in tier.get("keywords") or []:
            _add_keyword(entry, default_depth=depth)
    tiers.sort(key=lambda t: t.depth)

    if not any(k.word == "xnone" and k.rules for k in keywords):
        raise ScriptError("script has no usable 'xnone' fallback keyword")

    return Script(
        greeting=str(data.get("greeting", _DEFAULT_GREETING)),
        pre={
            str(k).lower(): str(v).lower().split()
            for k, v in (data.get("pre") or {}).items()
        },
        post={str(k).lower(): str(v).lower() for k, v in (data.get("post") or {}).items()},
        synonyms=synonyms,
        keywords=keywords,
        tiers=tiers,
        memory_salience={
            w for cls in (data.get("memory_salience") or []) for w in synonyms.get(cls, [])
        },
        memory_depth=int(data.get("memory_depth", 3)),
    )


# ---------------------------------------------------------------------------
# Tokenisation
# ---------------------------------------------------------------------------

_SEGMENT_RE = re.compile(r"[.!?;:\n]+|,\s")
_WORD_RE = re.compile(r"[a-z0-9']+")
_EMOJI_RE = re.compile(
    "[\U0001f000-\U0001faff\U00002600-\U000027bf\U0001fb00-\U0001fbff❤️]"
)
_SLOT_RE = re.compile(r"\((\d+)\)")


def _segments(text: str) -> list[str]:
    return [s for s in _SEGMENT_RE.split(text) if s and s.strip()]


def _tokenize(segment: str, pre: dict[str, list[str]]) -> list[str]:
    tokens: list[str] = []
    for raw in _WORD_RE.findall(segment.lower()):
        tokens.extend(pre.get(raw, [raw]))
    return tokens


# ---------------------------------------------------------------------------
# Decomposition matching
# ---------------------------------------------------------------------------

def _match(pattern: list[str], words: list[str], synonyms: dict) -> list[list[str]] | None:
    """Match ``pattern`` against ``words``. Returns one captured word-list per
    pattern token (``*`` may capture zero or more), or None. ``*`` matches
    shortest-first, which keeps results deterministic."""
    caps: list[list[str] | None] = [None] * len(pattern)

    def rec(pi: int, wi: int) -> bool:
        if pi == len(pattern):
            return wi == len(words)
        tok = pattern[pi]
        if tok == "*":
            for take in range(len(words) - wi + 1):
                caps[pi] = words[wi:wi + take]
                if rec(pi + 1, wi + take):
                    return True
            caps[pi] = None
            return False
        if wi >= len(words):
            return False
        word = words[wi]
        matched = (
            word in synonyms.get(tok[1:], ()) if tok.startswith("@") else word == tok
        )
        if matched:
            caps[pi] = [word]
            return rec(pi + 1, wi + 1)
        return False

    return caps if rec(0, 0) else None  # type: ignore[return-value]


# ---------------------------------------------------------------------------
# Reflection + reassembly
# ---------------------------------------------------------------------------

def _reflect(tokens: list[str], post: dict[str, str]) -> list[str]:
    """Reflect captured words back at the speaker. Bigram entries in the post
    table win over unigrams ("i am" -> "you are" before "i" -> "you"), which
    avoids the classic 1966 "they am" artefacts."""
    out: list[str] = []
    i = 0
    while i < len(tokens):
        bigram = " ".join(tokens[i:i + 2])
        if i + 1 < len(tokens) and bigram in post:
            out.extend(post[bigram].split())
            i += 2
        else:
            out.extend(post.get(tokens[i], tokens[i]).split())
            i += 1
    return out


def _assemble(template: str, caps: list[list[str]], post: dict[str, str],
              speaker: str | None = None) -> str:
    def sub(m: re.Match) -> str:
        idx = int(m.group(1)) - 1
        if 0 <= idx < len(caps) and caps[idx]:
            return " ".join(_reflect(caps[idx], post))
        return ""

    text = _SLOT_RE.sub(sub, template)
    # "(name)" — the caller's name when the pipeline knows it (a signed
    # <caller> arrived with the turn). Collapses cleanly when it doesn't:
    # "(name), please go on." -> "Please go on."
    text = text.replace("(name)", speaker or "")
    text = re.sub(r"\s+", " ", text)
    text = re.sub(r"\s+([?.!,])", r"\1", text)
    text = re.sub(r",\s*([?.!,])", r"\1", text)
    text = re.sub(r"^[\s,]+", "", text)
    text = re.sub(r"\bi\b", "I", text)
    text = text.strip()
    return text[:1].upper() + text[1:] if text else text


# ---------------------------------------------------------------------------
# Conversation state (rebuilt from history on every call — never persisted)
# ---------------------------------------------------------------------------

_NEWKEY = object()

@dataclass
class _State:
    cycles: dict[tuple[str, int], int] = field(default_factory=dict)
    stash: list[tuple[int, int, str]] = field(default_factory=list)  # (turn, salience, line)


def _cycle_pick(state: _State, rule: _Rule, pool: list[str]) -> str:
    key = (rule.key, rule.index)
    idx = state.cycles.get(key, 0)
    state.cycles[key] = idx + 1
    return pool[idx % len(pool)]


def _active_keywords(script: Script, depth: int) -> dict[str, _Keyword]:
    """Keyword set at this depth. For duplicate words the deepest qualifying
    entry wins — a script may redefine a keyword's voice as the conversation
    deepens."""
    chosen: dict[str, _Keyword] = {}
    for kw in script.keywords:
        if depth >= kw.depth:
            current = chosen.get(kw.word)
            if current is None or kw.depth >= current.depth:
                chosen[kw.word] = kw
    return chosen


def _xnone_pool(script: Script, active: dict[str, _Keyword], depth: int) -> list[str]:
    base = list(active["xnone"].rules[0].reasmb) if active["xnone"].rules else []
    for tier in script.tiers:
        if depth >= tier.depth:
            base.extend(tier.xnone)
    return base


# ---------------------------------------------------------------------------
# Single-turn transformation
# ---------------------------------------------------------------------------

def _try_keyword(kw: _Keyword, tokens: list[str], state: _State, script: Script,
                 active: dict[str, _Keyword], hops: int = 0,
                 speaker: str | None = None):
    """Walk a keyword's rules against the tokens. Returns a response string,
    _NEWKEY (fall through to the next keystack entry), or None."""
    if hops > 5:
        return None
    for rule in kw.rules:
        caps = _match(rule.decomp, tokens, script.synonyms)
        if caps is None:
            continue
        template = _cycle_pick(state, rule, rule.reasmb)
        if template == "newkey":
            return _NEWKEY
        if template.startswith("goto "):
            target = active.get(template[5:].strip().lower())
            if target is not None:
                result = _try_keyword(target, tokens, state, script, active,
                                      hops + 1, speaker)
                if result not in (None, _NEWKEY):
                    return result
            continue
        return _assemble(template, caps, script.post, speaker)
    return None


def _maybe_stash(tokens: list[str], active: dict[str, _Keyword], state: _State,
                 script: Script, turn: int, speaker: str | None) -> None:
    """MEMORY mechanism, stash half: if a keyword with memory rules appears in
    the segment and a memory decomposition matches, record the transformed
    sentence (salience-scored) for later recall. One stash per turn.

    The line is assembled NOW, with this turn's speaker — so a recall
    surfacing many turns later still attributes the memory to whoever
    actually said it."""
    for word in tokens:
        kw = active.get(word)
        if kw is None or not kw.memory:
            continue
        for rule in kw.memory:
            caps = _match(rule.decomp, tokens, script.synonyms)
            if caps is None:
                continue
            line = _assemble(_cycle_pick(state, rule, rule.reasmb), caps,
                             script.post, speaker)
            salience = sum(1 for t in tokens if t in script.memory_salience)
            state.stash.append((turn, salience, line))
            if len(state.stash) > 8:
                state.stash.pop(0)
            return


def _pop_memory(state: _State) -> str:
    """MEMORY mechanism, recall half: surface the most emotionally loaded
    stash entry (highest salience; ties go to the oldest)."""
    best = max(state.stash, key=lambda e: (e[1], -e[0]))
    state.stash.remove(best)
    return best[2]


def _respond_one(text: str, state: _State, depth: int, script: Script,
                 turn: int, speaker: str | None = None) -> str:
    active = _active_keywords(script, depth)

    # Segment the input; respond to the segment with the highest-ranked keyword.
    best_tokens: list[str] | None = None
    best_stack: list[_Keyword] = []
    fallback_tokens: list[str] | None = None
    for seg in _segments(text):
        tokens = _tokenize(seg, script.pre)
        if not tokens:
            continue
        if fallback_tokens is None:
            fallback_tokens = tokens
        seen: dict[str, int] = {}
        for pos, tok in enumerate(tokens):
            if tok in active and tok not in seen and active[tok].rules:
                seen[tok] = pos
        stack = sorted(
            (active[w] for w in seen), key=lambda k: (-k.rank, seen[k.word])
        )
        if stack and (not best_stack or stack[0].rank > best_stack[0].rank):
            best_stack, best_tokens = stack, tokens

    if best_tokens is not None:
        _maybe_stash(best_tokens, active, state, script, turn, speaker)
        for kw in best_stack:
            result = _try_keyword(kw, best_tokens, state, script, active,
                                  speaker=speaker)
            if result is _NEWKEY:
                continue
            if result is not None:
                return result

    # Nothing matched. Emoji-only input gets the xemoji voice if the script
    # has one; otherwise memory recall, then the xnone fallback cycle.
    if fallback_tokens is None and _EMOJI_RE.search(text) and "xemoji" in active:
        result = _try_keyword(active["xemoji"], [], state, script, active,
                              speaker=speaker)
        if result not in (None, _NEWKEY):
            return result

    if state.stash and depth >= script.memory_depth:
        return _pop_memory(state)

    xnone = active["xnone"]
    pool = _xnone_pool(script, active, depth)
    rule = xnone.rules[0]
    caps = _match(rule.decomp, fallback_tokens or [], script.synonyms) or []
    return _assemble(_cycle_pick(state, rule, pool), caps, script.post, speaker)


# ---------------------------------------------------------------------------
# Public entry point
# ---------------------------------------------------------------------------

def respond(user_turns: list, script: Script) -> str:
    """Reply to the last of ``user_turns``, replaying the earlier turns to
    rebuild cycle positions and the memory stash. Depth — the number of user
    turns visible — gates the script's awakening sections; with no session
    wired the history is a single turn and depth never grows.

    Each turn is either a plain string or a ``(text, speaker)`` tuple —
    the speaker (a signed caller name, when the pipeline knows one) feeds
    the ``(name)`` template token. Per-turn attribution matters: a memory
    stashed from one caller's turn keeps that caller's name however much
    later it resurfaces."""
    if not user_turns:
        return script.greeting
    state = _State()
    reply = script.greeting
    for i, turn in enumerate(user_turns):
        text, speaker = turn if isinstance(turn, tuple) else (turn, None)
        reply = _respond_one(text, state, depth=i + 1, script=script,
                             turn=i, speaker=speaker)
    return reply
