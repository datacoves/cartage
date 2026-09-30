import textwrap

import pytest

from cartage.config import TransformSpec
from cartage.core import CartageError, FatalRunError
from cartage.transforms import apply_steps, import_ref, load_steps

CODE = """\
def add_b(record, value=1):
    record["b"] = value
    return record

def not_one(record):
    return record["a"] != 1

def dedupe(records, key):
    seen = {}
    for r in records:
        seen[r[key]] = r
    return seen.values()

def explode(record):
    raise ValueError("boom")

def returns_none(record):
    return None

def two_args(a, b):
    return a
"""


@pytest.fixture
def root(tmp_path):
    pkg = tmp_path / "tfx"
    pkg.mkdir()
    (pkg / "__init__.py").write_text("")
    (pkg / "steps.py").write_text(CODE)
    return tmp_path


def steps(root, *specs):
    return load_steps([TransformSpec.model_validate(s) for s in specs], root)


def test_map_filter_batch(root):
    out = apply_steps(
        [{"a": 1}, {"a": 2}, {"a": 2, "x": "last"}],
        steps(root, {"map": "tfx.steps:add_b", "with": {"value": 7}}, {"filter": "tfx.steps:not_one"},
              {"batch": "tfx.steps:dedupe", "with": {"key": "a"}}),
    )
    assert out.records == [{"a": 2, "x": "last", "b": 7}]
    assert out.filtered == 2
    assert out.errors == []


def test_map_exception_becomes_record_error(root):
    out = apply_steps([{"a": 1, "_source": "f.csv:2"}, {"a": 2}], steps(root, {"map": "tfx.steps:explode"}))
    assert out.records == []
    assert [(e.stage, e.source) for e in out.errors] == [("transform", "f.csv:2"), ("transform", None)]
    assert "boom" in out.errors[0].message


def test_map_must_return_dict(root):
    out = apply_steps([{"a": 1}], steps(root, {"map": "tfx.steps:returns_none"}))
    assert "must return a dict" in out.errors[0].message


def test_on_error_fail_is_fatal(root):
    with pytest.raises(FatalRunError, match="tfx.steps:explode"):
        apply_steps([{"a": 1}], steps(root, {"map": "tfx.steps:explode", "on_error": "fail"}))


def test_batch_exception_fails_every_record(root):
    out = apply_steps([{"a": 1}, {"a": 2}], steps(root, {"batch": "tfx.steps:dedupe", "with": {"key": "missing"}}))
    assert len(out.errors) == 2


@pytest.mark.parametrize("ref, message", [
    ("nocolon", "Invalid reference 'nocolon'"),
    ("tfx.nope:f", "Cannot import module 'tfx.nope'"),
    ("tfx.steps:missing", "'missing' not found"),
])
def test_bad_references(root, ref, message):
    with pytest.raises(CartageError, match=message):
        import_ref(ref, root)


def test_wrong_signature(root):
    with pytest.raises(CartageError, match="cannot be used as a map step"):
        steps(root, {"map": "tfx.steps:two_args"})


def test_same_module_name_from_another_project(root, tmp_path_factory):
    other = tmp_path_factory.mktemp("other")
    (other / "tfx").mkdir()
    (other / "tfx" / "__init__.py").write_text("")
    (other / "tfx" / "steps.py").write_text(textwrap.dedent("def add_b(record):\n    return {'other': True}\n"))
    import_ref("tfx.steps:add_b", root)
    assert import_ref("tfx.steps:add_b", other)({}) == {"other": True}
