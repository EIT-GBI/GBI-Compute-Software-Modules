---
name: add-module
description: Add a new software module to this repo via the recipe templates — pick the template, render it, apply the expected post-render tweaks, test-install into ./usr, verify through Lmod, promote to the repo root, and update the docs. Use when asked to add or package a tool as a module, bump a version, or debug a recipe.
---

The playbook for this task is maintained tool-neutrally, shared with
non-Claude agents: read `docs/adding-modules.md` (from the repo root) now and
follow it. Repo-wide agent invariants live in `AGENTS.md`.

Do not duplicate content here — edit `docs/adding-modules.md` when the
workflow changes, so every agent vendor sees the same guidance.
