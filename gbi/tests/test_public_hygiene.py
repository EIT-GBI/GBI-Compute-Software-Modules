"""Real temporary Git fixtures exercise privacy findings and safe examples."""

import contextlib
import io
from pathlib import Path
import subprocess
import sys
import tempfile
import unittest
from unittest.mock import patch

import public_hygiene


class PublicHygiene(unittest.TestCase):
    def setUp(self):
        self.temporary = tempfile.TemporaryDirectory()
        self.addCleanup(self.temporary.cleanup)
        self.root = Path(self.temporary.name)
        subprocess.run(["git", "init", "-q", str(self.root)], check=True)
        self.add("gbi/src/lib/example.py", "print('public example')\n")

    def add(self, name, value):
        path = self.root / name
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(value)
        subprocess.run(["git", "add", "--", name], cwd=self.root, check=True)

    def test_safe_documentation_placeholders_and_local_fixtures(self):
        self.add("gbi/README.md", "\n".join((
            "/mnt/user-data/$USER/run", "/mnt/lustre/users/<username>/run",
            "/your/lustre/run", "https://github.com/example/project", "https://rclone.org",
            "127.0.0.1", "192.0.2.4", "198.51.100.8", "203.0.113.9", "rclone 1.75.1")))
        self.add("gbi/tests/example.py", "root = temporary_directory / 'alice'\n")
        count, findings = public_hygiene.scan_repository(self.root)
        self.assertEqual(count, 3)
        self.assertEqual(findings, [])

    def test_python_help_recipes_and_docs_are_scanned_without_disclosing_values(self):
        samples = {
            "gbi/src/lib/private.py": ("endpoint = '" + ".".join(("10", "23", "45", "67")) + "'", "private IPv4 address"),
            "gbi/sm-help": ("/mnt/user-data/" + "actual-person/run", "literal personal path"),
            "rclone/sm-config/settings.toml": ("id = '" + "ocid1." + "bucket.oc1.region.secret" + "'", "cloud resource identifier"),
            "gbi/src/lib/url.py": ("/n/" + "actualnamespace/b/actualbucket", "literal Object Storage namespace/bucket"),
            "gbi/README.md": ("-----BEGIN " + "RSA PRIVATE KEY-----", "private key"),
        }
        for name, (value, _) in samples.items():
            self.add(name, value)
        _, findings = public_hygiene.scan_repository(self.root)
        self.assertEqual({(name, rule) for name, _, rule in findings},
                         {(name, rule) for name, (_, rule) in samples.items()})
        with patch.object(public_hygiene, "scan_repository", return_value=(6, findings)), contextlib.redirect_stdout(io.StringIO()) as output:
            self.assertEqual(public_hygiene.main(), 1)
        for value, _ in samples.values():
            self.assertNotIn(value, output.getvalue())

    def test_generated_configuration_is_rejected_but_template_is_allowed(self):
        self.add("gbi/site.conf", "bucket_root=/configured/location\n")
        self.add("gbi/site.conf.example", "bucket_root=<site-root>\n")
        _, findings = public_hygiene.scan_repository(self.root)
        self.assertEqual(findings, [("gbi/site.conf", 0, "generated site configuration must not be committed")])

    def test_empty_checkout_cannot_pass(self):
        subprocess.run(["git", "rm", "-q", "--cached", "gbi/src/lib/example.py"], cwd=self.root, check=True)
        with self.assertRaisesRegex(ValueError, "no tracked gbi/src"):
            public_hygiene.scan_repository(self.root)

    def test_unreadable_text_and_symlink_do_not_silently_pass(self):
        self.add("gbi/binary", "placeholder")
        (self.root / "gbi/binary").write_bytes(b"\xff\x00")
        (self.root / "gbi/link").symlink_to("../outside-private-file")
        subprocess.run(["git", "add", "gbi/link"], cwd=self.root, check=True)
        _, findings = public_hygiene.scan_repository(self.root)
        self.assertEqual(len(findings), 2)

    def test_generated_pdf_metadata_and_annotation_are_scanned(self):
        try:
            from pypdf import PdfWriter
            from pypdf.generic import (ArrayObject, DictionaryObject,
                                       NameObject, NumberObject,
                                       TextStringObject)
        except ImportError:
            self.skipTest("pypdf is installed by the hygiene workflow")
        writer = PdfWriter()
        page = writer.add_blank_page(width=100, height=100)
        writer.add_metadata({"/Title": "Public guide"})
        annotation = DictionaryObject({
            NameObject("/Type"): NameObject("/Annot"),
            NameObject("/Subtype"): NameObject("/Link"),
            NameObject("/Rect"): ArrayObject(
                [NumberObject(0), NumberObject(0), NumberObject(10),
                 NumberObject(10)]),
            NameObject("/A"): DictionaryObject({
                NameObject("/S"): NameObject("/URI"),
                NameObject("/URI"): TextStringObject(
                    "/n/" + "actualnamespace/b/actualbucket"),
            }),
        })
        page[NameObject("/Annots")] = ArrayObject([annotation])
        path = self.root / "gbi/guide.pdf"
        with path.open("wb") as stream:
            writer.write(stream)
        subprocess.run(["git", "add", "--", str(path)], cwd=self.root,
                       check=True)
        _, findings = public_hygiene.scan_repository(self.root)
        self.assertEqual(findings, [("gbi/guide.pdf", 1,
                                     "literal Object Storage namespace/bucket")])

    def test_generated_pdf_private_metadata_is_reported(self):
        try:
            from pypdf import PdfWriter
        except ImportError:
            self.skipTest("pypdf is installed by the hygiene workflow")
        writer = PdfWriter()
        writer.add_blank_page(width=100, height=100)
        writer.add_metadata({"/Author": "/mnt/user-data/" + "alice/run"})
        path = self.root / "gbi/private.pdf"
        with path.open("wb") as stream:
            writer.write(stream)
        subprocess.run(["git", "add", "--", str(path)], cwd=self.root,
                       check=True)
        _, findings = public_hygiene.scan_repository(self.root)
        self.assertEqual(findings, [("gbi/private.pdf", 0,
                                     "literal personal path")])

    def test_generated_pdf_text_stream_and_safe_text_are_scanned(self):
        try:
            from pypdf import PdfWriter
            from pypdf.generic import (DecodedStreamObject, DictionaryObject,
                                       NameObject)
        except ImportError:
            self.skipTest("pypdf is installed by the hygiene workflow")

        def write_text_pdf(path, value):
            writer = PdfWriter()
            page = writer.add_blank_page(width=100, height=100)
            font = DictionaryObject({
                NameObject("/Type"): NameObject("/Font"),
                NameObject("/Subtype"): NameObject("/Type1"),
                NameObject("/BaseFont"): NameObject("/Helvetica"),
            })
            page[NameObject("/Resources")] = DictionaryObject({
                NameObject("/Font"): DictionaryObject({
                    NameObject("/F1"): writer._add_object(font),
                }),
            })
            stream = DecodedStreamObject()
            stream.set_data(("BT /F1 12 Tf 10 50 Td (" + value + ") Tj ET")
                            .encode("ascii"))
            page[NameObject("/Contents")] = writer._add_object(stream)
            with path.open("wb") as output:
                writer.write(output)
            subprocess.run(["git", "add", "--", str(path)], cwd=self.root,
                           check=True)

        private_path = self.root / "gbi/text-private.pdf"
        safe_path = self.root / "gbi/text-safe.pdf"
        write_text_pdf(private_path, "/mnt/user-data/" + "alice/run")
        write_text_pdf(safe_path, "/your/lustre/" + "run")
        _, findings = public_hygiene.scan_repository(self.root)
        self.assertEqual(findings, [("gbi/text-private.pdf", 1,
                                     "literal personal path")])

    def test_invalid_pdf_fails_closed(self):
        try:
            import pypdf  # noqa: F401
        except ImportError:
            self.skipTest("invalid-PDF parsing requires pypdf")
        self.add("gbi/broken.pdf", "%PDF-1.7\nnot a PDF\n")
        _, findings = public_hygiene.scan_repository(self.root)
        self.assertEqual(findings, [("gbi/broken.pdf", 0,
                                     "PDF could not be parsed safely")])

    def test_missing_pdf_parser_fails_closed(self):
        path = self.root / "gbi/no-parser.pdf"
        path.write_bytes(b"not a PDF")
        subprocess.run(["git", "add", "--", str(path)], cwd=self.root,
                       check=True)
        with patch.dict(sys.modules, {"pypdf": None}):
            _, findings = public_hygiene.scan_repository(self.root)
        self.assertEqual(findings, [("gbi/no-parser.pdf", 0,
                                     "PDF parser unavailable; cannot scan safely")])


if __name__ == "__main__":
    unittest.main()
