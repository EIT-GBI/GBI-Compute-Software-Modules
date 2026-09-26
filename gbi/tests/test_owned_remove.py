"""Owned cleanup never follows a swapped parent to unrelated data."""

import os
from pathlib import Path

import pytest

from gbi_data import formats


@pytest.fixture
def owned(tmp_path):
    root = tmp_path / "root"
    parent = root / "stage"
    parent.mkdir(parents=True)
    payload = parent / "payload"
    payload.write_bytes(b"owned")
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / payload.name).write_bytes(b"unrelated")
    return root, payload, outside


def test_parent_swap_immediately_before_unlink_cannot_redirect_cleanup(owned, monkeypatch):
    root, payload, outside = owned
    identity = formats._identity(payload)
    retained = root / "retained"
    unlink = os.unlink

    def swap_then_unlink(path, *, dir_fd=None):
        payload.parent.rename(retained)
        payload.parent.symlink_to(outside, target_is_directory=True)
        return unlink(path, dir_fd=dir_fd)

    with monkeypatch.context() as patch:
        patch.setattr(formats.os, "unlink", swap_then_unlink)
        formats._owned_remove(payload, identity, root)
    assert (outside / payload.name).read_bytes() == b"unrelated"
    assert not (retained / payload.name).exists()


def test_parent_swap_after_path_check_is_rejected_without_deletion(owned, monkeypatch):
    root, payload, outside = owned
    identity = formats._identity(payload)
    retained = root / "retained"
    check_parent = formats.check_parent

    def swap_after_check(path, boundary):
        check_parent(path, boundary)
        payload.parent.rename(retained)
        payload.parent.symlink_to(outside, target_is_directory=True)

    monkeypatch.setattr(formats, "check_parent", swap_after_check)
    with pytest.raises(OSError):
        formats._owned_remove(payload, identity, root)
    assert (outside / payload.name).read_bytes() == b"unrelated"
    assert (retained / payload.name).read_bytes() == b"owned"


@pytest.mark.parametrize("replacement", ["inode", "symlink", "hardlink", "directory"])
def test_changed_owned_entry_is_retained(owned, replacement):
    root, payload, outside = owned
    identity = formats._identity(payload)
    if replacement == "hardlink":
        os.link(payload, payload.with_name("alias"))
    else:
        payload.rename(payload.with_name("original"))
        if replacement == "inode":
            payload.write_bytes(b"replacement")
        elif replacement == "symlink":
            payload.symlink_to(outside / payload.name)
        else:
            payload.mkdir()
    with pytest.raises(ValueError, match="changed; retained"):
        formats._owned_remove(payload, identity, root)
    assert os.path.lexists(payload)
    assert (outside / payload.name).read_bytes() == b"unrelated"


def test_outside_root_is_rejected(owned):
    root, payload, outside = owned
    target = outside / payload.name
    with pytest.raises(ValueError, match="parent path"):
        formats._owned_remove(target, formats._identity(target), root)
    assert target.read_bytes() == b"unrelated"


def test_unchanged_owned_file_is_removed(owned):
    root, payload, outside = owned
    formats._owned_remove(payload, formats._identity(payload), root)
    assert not payload.exists()
    assert (outside / payload.name).read_bytes() == b"unrelated"


@pytest.fixture
def staged_task(tmp_path):
    source = tmp_path / "source"
    target = tmp_path / "target"
    scratch = tmp_path / "lustre"
    state = scratch / ".gbi/state"
    for path in (source, target, state / "locks", state / "pending"):
        path.mkdir(parents=True)
    (source / "file").write_bytes(b"payload")
    return {"format_action": "pack", "source": str(source),
            "format_target": str(target / "bundle.gbi.tar.gbi-chunks"),
            "source_root": str(tmp_path), "target_root": str(target),
            "source_kind": "fss", "target_kind": "alluxio", "state": str(state),
            "scratch_root": str(scratch), "receipt": str(tmp_path / "receipts.jsonl"),
            "progress": str(tmp_path / "progress.json"), "settle_seconds": 0,
            "delete": False, "pack": "tar", "chunk_size": 4096}


@pytest.mark.parametrize("interrupted", [True, False])
@pytest.mark.parametrize("replace_parent", [True, False])
def test_stage_swap_between_payload_and_journal_preserves_foreign_file(staged_task, tmp_path, monkeypatch, interrupted, replace_parent):
    if interrupted:
        def fail_pack(source, output, **options):
            output.write(b"partial")
            raise ValueError("interrupted staging")
        with monkeypatch.context() as patch:
            patch.setattr(formats.archives, "pack", fail_pack)
            with pytest.raises(ValueError, match="interrupted staging"):
                formats.transfer(staged_task)
    outside = tmp_path / "outside"
    outside.mkdir()
    (outside / "stage.json").write_bytes(b"foreign journal")
    retained = tmp_path / "retained-stage"
    remove = formats._owned_remove

    def remove_then_swap(path, identity, root):
        remove(path, identity, root)
        if path.name == "bundle.gbi.tar":
            if replace_parent:
                path.parent.rename(retained)
                path.parent.symlink_to(outside, target_is_directory=True)
            else:
                journal = path.parent / "stage.json"
                retained.mkdir()
                journal.rename(retained / journal.name)
                journal.write_bytes(b"foreign journal")

    monkeypatch.setattr(formats, "_owned_remove", remove_then_swap)
    with pytest.raises(ValueError, match="parent path|changed; retained"):
        formats.transfer(staged_task)
    assert (outside / "stage.json").read_bytes() == b"foreign journal"
    assert (retained / "stage.json").is_file()
    if not replace_parent:
        journal, = (Path(staged_task["state"]) / "format-staging").glob("*/stage.json")
        assert journal.read_bytes() == b"foreign journal"
    assert (Path(staged_task["source"]) / "file").read_bytes() == b"payload"


@pytest.mark.parametrize("replace_parent", [False, True])
def test_pending_journal_identity_is_captured_before_receipt_cleanup(staged_task, tmp_path, monkeypatch, replace_parent):
    task = {**staged_task, "chunk_size": None,
            "format_target": str(tmp_path / "target/bundle.gbi.tar")}
    original_receipt = formats.receipt
    changed = []

    def receipt_then_replace(path, record):
        original_receipt(path, record)
        pending = Path(task["state"]) / "pending"
        journal, = pending.glob("*.format.json")
        if replace_parent:
            outside = tmp_path / "outside"
            outside.mkdir()
            pending.rename(pending.with_name("retained-pending"))
            pending.symlink_to(outside, target_is_directory=True)
        else:
            journal.rename(journal.with_suffix(".retained"))
        journal.write_bytes(b"foreign journal")
        changed.append(journal)

    monkeypatch.setattr(formats, "receipt", receipt_then_replace)
    with pytest.raises(ValueError, match="changed; retained|parent path"):
        formats.transfer(task)
    assert len(changed) == 1
    assert changed[0].read_bytes() == b"foreign journal"


def test_directory_parent_swap_before_rmdir_cannot_redirect_cleanup(owned, monkeypatch):
    root, payload, outside = owned
    directory = payload.parent / "empty"
    directory.mkdir()
    (outside / directory.name).mkdir()
    identity = formats._identity(directory)
    retained = root / "retained"
    rmdir = os.rmdir

    def swap_then_rmdir(path, *, dir_fd=None):
        directory.parent.rename(retained)
        directory.parent.symlink_to(outside, target_is_directory=True)
        return rmdir(path, dir_fd=dir_fd)

    with monkeypatch.context() as patch:
        patch.setattr(formats.os, "rmdir", swap_then_rmdir)
        formats._owned_rmdir(directory, identity, root)
    assert (outside / directory.name).is_dir()
    assert not (retained / directory.name).exists()


@pytest.mark.parametrize("symlink", [False, True])
def test_foreign_directory_or_symlink_is_retained(owned, symlink):
    root, payload, outside = owned
    directory = payload.parent / "empty"
    directory.mkdir()
    identity = formats._identity(directory)
    directory.rename(directory.with_name("retained"))
    if symlink:
        directory.symlink_to(outside, target_is_directory=True)
    else:
        directory.mkdir()
    with pytest.raises(ValueError, match="changed; retained"):
        formats._owned_rmdir(directory, identity, root)
    assert directory.is_dir()
    assert outside.is_dir()


def test_stage_directory_identity_precedes_payload_cleanup(staged_task, tmp_path, monkeypatch):
    remove = formats._owned_remove
    retained = tmp_path / "retained-stage"
    changed = []

    def remove_then_replace_stage(path, identity, root):
        remove(path, identity, root)
        if path.name == "stage.json":
            path.parent.rename(retained)
            path.parent.mkdir()
            changed.append(path.parent)

    monkeypatch.setattr(formats, "_owned_remove", remove_then_replace_stage)
    with pytest.raises(ValueError, match="changed; retained"):
        formats.transfer(staged_task)
    assert len(changed) == 1
    assert changed[0].is_dir()
    assert retained.is_dir()
