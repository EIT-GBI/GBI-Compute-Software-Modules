# Public CLI hygiene

Run `python3 gbi/tests/public_hygiene.py` from a checkout. The GitHub Action runs
the same check and its fixture tests on pushes and pull requests, with read-only
repository permission and no cloud access. Python's standard library is the
only scanner dependency; the pinned official checkout action obtains the code.

The scan reads Git-tracked files under `gbi/`, `rclone/` and `.github/`, plus the
repository README and agent guide. That includes Python source/tests, module
recipes, command help and documentation. Vendored tools and unrelated module
recipes are outside this CLI check. An empty or unreadable scan fails.

It rejects recognisable cloud resource identifiers, literal Object Storage
namespace/bucket URL paths, private IPv4 addresses,
literal usernames in personal storage/workstation paths, private-key headers,
and generated `site.conf` files. Diagnostics contain the filename, line and rule,
not the matching content. Localhost, documentation address ranges, public URLs,
generic fixture names and `$USER`/`<username>` path placeholders are allowed.
Runtime configuration remains injected by the maintained install recipe.

This is a regression guard, not a complete secret detector. Arbitrary usernames,
bucket names, namespaces or hostnames cannot reliably be recognised without
private inventory, which must not be embedded in this public repository.
Reviewers must still check examples and configuration for real site data. The
check does not scan Git history or certify unrelated vendored code.

Run the fixture tests with:

```sh
python3 -m unittest discover -s gbi/tests -p test_public_hygiene.py
```

Fixtures exercise allowed examples, Python/help/recipe/documentation leaks,
generated configuration, empty scope, unreadable text and symlinks. Temporary
repositories use the caller's `TMPDIR`; set it inside your permitted workspace.
