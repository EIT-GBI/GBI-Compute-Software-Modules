"""Maintained retained staging accepts caller-relative selection context."""

from pathlib import Path
from types import SimpleNamespace

import pytest
from gbi_data import archives, formats


@pytest.fixture
def inputs(tmp_path):
    source = tmp_path / "source"
    source.mkdir()
    (source / "a.txt").write_bytes(b"selected")
    (source / "b.txt").write_bytes(b"excluded")
    scratch = tmp_path / "lustre"
    state = scratch / ".gbi/state"
    state.mkdir(parents=True)
    task = {"scratch_root": str(scratch), "pack": "tar", "include": ["outer/*.txt"],
            "exclude": ["outer/b.txt"], "selection_mode": "relative", "selection_prefix": "outer"}
    return source, state, task


def test_relative_stage_and_retained_resume_preserve_member_paths(inputs):
    source, state, task = inputs
    first = formats._stage_archive(task, source, Path("bundle.gbi.tar.gbi-chunks"), state, "key", lambda *args: None)
    payload, manifest, saved, _ = first
    assert [entry["path"] for entry in manifest["entries"]] == ["a.txt"]
    assert archives.inspect_archive(payload)["entries"] == formats._public_manifest(manifest)["entries"]
    second = formats._stage_archive(task, source, Path("bundle.gbi.tar.gbi-chunks"), state, "key", lambda *args: None)
    assert second[2]["owned"] == saved["owned"]
    assert second[0].stat().st_ino == payload.stat().st_ino


def test_relative_capacity_estimate_counts_selected_payload_before_stage_write(inputs, monkeypatch):
    source, state, task = inputs
    (source / "large.bin").write_bytes(b"x" * (2 << 20))
    task.update(include=["outer/large.bin"], exclude=[])
    monkeypatch.setattr(formats.shutil, "disk_usage", lambda _path: SimpleNamespace(free=68 << 20))
    with pytest.raises(ValueError, match="not enough Lustre scratch"):
        formats._stage_archive(task, source, Path("bundle.gbi.tar.gbi-chunks"), state, "key", lambda *args: None)
    assert not (state / "format-staging/key/bundle.gbi.tar").exists()
    assert (source / "large.bin").exists()


def test_default_pack_options_do_not_change_existing_journal_intents():
    assert "selection_mode" not in formats._pack_options({})
    assert "selection_prefix" not in formats._pack_options({})
