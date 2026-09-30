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


def test_namespace_package_isolation(tmp_path_factory):
    """Namespace packages (no __init__.py) from different roots are isolated."""
    root1 = tmp_path_factory.mktemp("root1")
    tfxns1 = root1 / "tfxns"
    tfxns1.mkdir()
    (tfxns1 / "steps.py").write_text("def get_root():\n    return {'root': 1}\n")

    root2 = tmp_path_factory.mktemp("root2")
    tfxns2 = root2 / "tfxns"
    tfxns2.mkdir()
    (tfxns2 / "steps.py").write_text("def get_root():\n    return {'root': 2}\n")

    func1 = import_ref("tfxns.steps:get_root", root1)
    func2 = import_ref("tfxns.steps:get_root", root2)

    assert func1() == {"root": 1}
    assert func2() == {"root": 2}


def test_single_file_module_isolation(tmp_path_factory):
    """Single-file modules from different roots are isolated."""
    root1 = tmp_path_factory.mktemp("root1")
    (root1 / "onefile.py").write_text("def get_source():\n    return {'source': 1}\n")

    root2 = tmp_path_factory.mktemp("root2")
    (root2 / "onefile.py").write_text("def get_source():\n    return {'source': 2}\n")

    func1 = import_ref("onefile:get_source", root1)
    func2 = import_ref("onefile:get_source", root2)

    assert func1() == {"source": 1}
    assert func2() == {"source": 2}


def test_batch_step_yields_non_dict_items(root):
    """Batch step yielding non-dict items creates errors; dicts pass through."""
    code = textwrap.dedent("""\
        def mixed_yield(records):
            for r in records:
                yield r if r["a"] == 1 else "not a dict"
    """)
    (root / "tfx" / "steps.py").write_text(code)

    out = apply_steps(
        [{"a": 1}, {"a": 2}],
        steps(root, {"batch": "tfx.steps:mixed_yield"}),
    )
    assert out.records == [{"a": 1}]
    assert len(out.errors) == 1
    assert "a batch step must yield dicts, got str" in out.errors[0].message


def test_map_returning_new_dict_keeps_source():
    from cartage.transforms import Step
    step = Step("map", "x:rename", lambda r: {"id": r["a"]}, {}, "record")
    out = apply_steps([{"a": 1, "_source": "f.csv:2"}, {"a": 2}], [step])
    assert out.records == [{"id": 1, "_source": "f.csv:2"}, {"id": 2}]
