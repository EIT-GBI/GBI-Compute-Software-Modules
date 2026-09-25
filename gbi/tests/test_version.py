"""Release version declarations stay aligned."""

from pathlib import Path
import ast
import re
import unittest

from gbi_data import __version__


class VersionDeclarations(unittest.TestCase):
    def test_source_recipe_and_package_versions_match(self):
        root = Path(__file__).parents[1]
        self.assertEqual((root / "VERSION").read_text().strip(), __version__)
        settings = (root / "sm-config" / "settings.toml").read_text()
        versions = re.search(r"(?m)^versions\s*=\s*(\[.*\])$", settings)
        self.assertIsNotNone(versions)
        self.assertEqual(ast.literal_eval(versions.group(1)), [__version__])


if __name__ == "__main__":
    unittest.main()
