"""deterministic_id: stable, order-sensitive, collision-resistant enough.

The example tests below pin the behaviour the ids rely on; the properties state
it over the whole input space. The three ``xfail`` properties record where the
current construction does *not* hold — distinct inputs that share an id — and
are strict, so fixing the hash turns them into failures until the markers go.
"""

from __future__ import annotations

import pytest
from hypothesis import assume, given
from hypothesis import strategies as st

from data_pipeline_core import deterministic_id
from data_pipeline_core.storage.ids import _SEP

# Parts that cannot themselves contain a part boundary — i.e. the domain the
# current construction is safe over. Tracks _SEP so swapping the separator
# re-aims the strategy with it.
_SAFE_PART = st.text(alphabet=st.characters(exclude_characters=_SEP), max_size=8)
_SAFE_PARTS = st.lists(_SAFE_PART, min_size=1, max_size=5)
# Parts drawn from an alphabet that *can* produce the separator.
_INJECTING_PARTS = st.lists(
    st.text(alphabet=st.sampled_from(["a", "b", _SEP]), max_size=4),
    min_size=2,
    max_size=4,
)
# What actually gets hashed: bookmaker slugs, competition and team names, ISO
# timestamps, market codes. Hyphens, colons, dots and accents are all ordinary
# here — which is what the separator-choice property below depends on.
_REALISTIC_PART = st.one_of(
    st.sampled_from(
        [
            "betclic",
            "Ligue 1",
            "Saint-Étienne",
            "Bayer 04 Leverkusen",
            "1x2",
            "2026-08-15T19:00:00Z",
        ]
    ),
    st.text(
        alphabet=st.characters(
            categories=("Lu", "Ll", "Nd"), include_characters=" -:.'"
        ),
        min_size=1,
        max_size=12,
    ),
)


def test_stable_across_calls() -> None:
    assert deterministic_id("betclic", "OM", "OL", "1x2") == deterministic_id(
        "betclic", "OM", "OL", "1x2"
    )


def test_order_sensitive() -> None:
    assert deterministic_id("a", "b") != deterministic_id("b", "a")


def test_distinct_inputs_differ() -> None:
    assert deterministic_id("a", "b") != deterministic_id("a", "c")


def test_none_and_empty_are_distinct_from_values() -> None:
    assert deterministic_id("a", None) != deterministic_id("a", "None")
    assert deterministic_id("a", "") != deterministic_id("a")


def test_lone_surrogates_hash_instead_of_raising() -> None:
    # json.loads decodes an unpaired \uD800-\uDFFF escape without complaint, so a
    # mangled upstream payload reaches us as a lone surrogate. Minting an id must
    # not be what fails the run.
    assert deterministic_id("\ud800") != deterministic_id("\udfff")
    assert deterministic_id("a", "\ud800") == deterministic_id("a", "\ud800")


@given(_SAFE_PARTS)
def test_is_deterministic(parts: list[str]) -> None:
    assert deterministic_id(*parts) == deterministic_id(*parts)


@given(st.lists(_SAFE_PART, min_size=2, max_size=5, unique=True))
def test_reordering_parts_changes_the_id(parts: list[str]) -> None:
    assert deterministic_id(*parts) != deterministic_id(*reversed(parts))


@given(_SAFE_PARTS, _SAFE_PARTS)
def test_separator_free_parts_never_collide(a: list[str], b: list[str]) -> None:
    # With no part containing _SEP the joined payload is uniquely decodable, so
    # distinct part-tuples give distinct payloads and distinct digests.
    assume(a != b)
    assert deterministic_id(*a) != deterministic_id(*b)


@given(_REALISTIC_PART)
def test_the_separator_cannot_occur_in_a_field_value(part: str) -> None:
    # The construction is only collision-free because no part can contain the
    # boundary. That is an assumption about the *data*, so assert it directly:
    # pick a separator that occurs in real field values (a hyphen, say) and the
    # ids silently start colliding on team names and ISO dates.
    assert _SEP not in part


@pytest.mark.xfail(
    strict=True,
    reason="_SEP is joined, not escaped: a part containing it is indistinguishable "
    "from a part boundary, so ('a', 'b') and ('a\\x1fb',) share an id. Safe today "
    "only because bookmaker field values never contain \\x1f — the guarantee is a "
    "property of the data, not of the function. Fixing it rewrites every id ever "
    "minted, so it is a migration, not a patch.",
)
@given(_INJECTING_PARTS)
def test_a_part_containing_the_separator_never_collides(parts: list[str]) -> None:
    assert deterministic_id(*parts) != deterministic_id(_SEP.join(parts))


@pytest.mark.xfail(
    strict=True,
    reason="None is rendered as the empty string, so ('a', None) and ('a', '') "
    "share an id. A missing field and a blank one are not the same observation.",
)
def test_none_is_distinct_from_the_empty_string() -> None:
    assert deterministic_id("a", None) != deterministic_id("a", "")


@pytest.mark.xfail(
    strict=True,
    reason="parts are stringified, so 1 and '1' — and True and 'True' — share an id.",
)
def test_types_are_distinguished() -> None:
    assert deterministic_id(1) != deterministic_id("1")
