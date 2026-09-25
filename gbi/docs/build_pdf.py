# /// script
# requires-python = ">=3.10"
# dependencies = ["reportlab>=4,<6", "markdown-it-py>=3,<5"]
# ///
"""Render the maintained Markdown guide: uv run gbi/docs/build_pdf.py."""

import argparse
from datetime import date
from html import escape
from pathlib import Path
import re

from markdown_it import MarkdownIt
from reportlab.lib import colors
from reportlab.lib.enums import TA_LEFT
from reportlab.lib.pagesizes import A4
from reportlab.lib.styles import ParagraphStyle, getSampleStyleSheet
from reportlab.lib.units import mm
from reportlab.platypus import (
    BaseDocTemplate, Frame, PageBreak, PageTemplate, Paragraph,
    Preformatted, Spacer, Table, TableStyle,
)
from reportlab.platypus.tableofcontents import TableOfContents


NAVY = colors.HexColor("#173044")
TEAL = colors.HexColor("#007F80")
INK = colors.HexColor("#263746")
PALE = colors.HexColor("#EEF5F7")
WIDTH = A4[0] - 36 * mm


def plain(text):
    return text.replace("\u2011", "-").replace("\u2013", "-").replace("\u2014", " - ")


def inline(token):
    parts = []
    for child in token.children or []:
        kind = child.type
        if kind == "code_inline":
            parts.append('<font name="Courier" size="9">' + escape(plain(child.content)) + "</font>")
        elif kind in ("text", "html_inline"):
            parts.append(escape(plain(child.content)))
        elif kind == "softbreak":
            parts.append(" ")
        elif kind == "hardbreak":
            parts.append("<br/>")
        elif kind in ("strong_open", "strong_close", "em_open", "em_close"):
            parts.append({"strong_open": "<b>", "strong_close": "</b>",
                          "em_open": "<i>", "em_close": "</i>"}[kind])
        elif kind == "link_open":
            href = child.attrGet("href")
            if not href.startswith(("https://", "http://", "#")):
                href = "https://github.com/EIT-GBI/GBI-Compute-Software-Modules/blob/main/gbi/docs/" + href
            parts.append(f'<a href="{escape(href, quote=True)}" color="#007F80">')
        elif kind == "link_close":
            parts.append("</a>")
    return "".join(parts)


def styles():
    result = getSampleStyleSheet()
    result.add(ParagraphStyle("Body", fontName="Helvetica", fontSize=10.4,
                              leading=15, textColor=INK, spaceAfter=8))
    result.add(ParagraphStyle("Section", parent=result["Body"], fontName="Helvetica-Bold",
                              fontSize=18, leading=22, textColor=NAVY,
                              spaceBefore=17, spaceAfter=10, keepWithNext=True))
    result.add(ParagraphStyle("Subsection", parent=result["Section"], fontSize=12,
                              leading=16, spaceBefore=11, spaceAfter=7))
    result.add(ParagraphStyle("GuideCode", fontName="Courier", fontSize=8.3,
                              leading=11.5, textColor=NAVY, spaceAfter=0))
    result.add(ParagraphStyle("Cell", parent=result["Body"], fontSize=8.5,
                              leading=12, spaceAfter=0))
    result.add(ParagraphStyle("CellHead", parent=result["Cell"],
                              fontName="Helvetica-Bold", textColor=colors.white))
    result.add(ParagraphStyle("GuideBullet", parent=result["Body"], leftIndent=12,
                              bulletIndent=0, spaceAfter=5))
    result.add(ParagraphStyle("Callout", parent=result["Body"], borderColor=TEAL,
                              borderWidth=1, borderPadding=9, backColor=PALE))
    return result


class Guide(BaseDocTemplate):
    def __init__(self, output, version, published):
        super().__init__(str(output), pagesize=A4, leftMargin=18 * mm,
                         rightMargin=18 * mm, topMargin=20 * mm, bottomMargin=19 * mm,
                         title="GBI CLI and Python user guide", author="GBI Scientific Computing")
        self.version, self.published = version, published
        frame = Frame(self.leftMargin, self.bottomMargin, self.width, self.height,
                      leftPadding=0, rightPadding=0, topPadding=0, bottomPadding=0)
        self.addPageTemplates(PageTemplate("guide", frames=[frame], onPage=self.decorate))

    def decorate(self, canvas, doc):
        canvas.saveState()
        canvas.setFillColor(TEAL)
        canvas.rect(0, A4[1] - 5 * mm, A4[0], 5 * mm, fill=1, stroke=0)
        if doc.page > 1:
            canvas.setFont("Helvetica-Bold", 8)
            canvas.setFillColor(NAVY)
            canvas.drawString(18 * mm, A4[1] - 13 * mm, "GBI  /  CLI & PYTHON USER GUIDE")
        canvas.setStrokeColor(colors.HexColor("#D5E2E7"))
        canvas.line(18 * mm, 14 * mm, A4[0] - 18 * mm, 14 * mm)
        canvas.setFont("Helvetica", 8)
        canvas.setFillColor(INK)
        canvas.drawString(18 * mm, 9 * mm, f"GBI {self.version}  |  {self.published}")
        canvas.drawRightString(A4[0] - 18 * mm, 9 * mm, str(doc.page))
        canvas.restoreState()

    def afterFlowable(self, flowable):
        if getattr(flowable, "guide_anchor", None):
            anchor = flowable.guide_anchor
            self.canv.bookmarkPage(anchor)
            self.canv.addOutlineEntry(flowable.getPlainText(), anchor, level=0)
            self.notify("TOCEntry", (0, flowable.getPlainText(), self.page, anchor))


def content(markdown, style):
    tokens = MarkdownIt("commonmark").enable("table").parse(markdown)
    story, lists = [], []
    index, bullet, quote = 0, None, False
    while index < len(tokens):
        token = tokens[index]
        if token.type == "heading_open":
            level = int(token.tag[1:])
            item = tokens[index + 1]
            if level > 1:
                para = Paragraph(inline(item), style["Section" if level == 2 else "Subsection"])
                if level == 2:
                    para.guide_anchor = re.sub(r"[^a-z0-9]+", "-", item.content.lower()).strip("-")
                story.append(para)
            index += 3
            continue
        if token.type == "paragraph_open":
            name = "GuideBullet" if bullet else "Callout" if quote else "Body"
            story.append(Paragraph(inline(tokens[index + 1]), style[name], bulletText=bullet))
            bullet = None
            index += 3
            continue
        if token.type == "fence":
            text = plain(token.content).rstrip()
            if any(len(line.expandtabs(4)) > 90 for line in text.splitlines()):
                raise ValueError("code example exceeds the PDF line budget; shorten it in Markdown")
            block = Table([[Preformatted(text, style["GuideCode"]) ]], colWidths=[WIDTH])
            block.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, -1), PALE),
                ("BOX", (0, 0), (-1, -1), 0.4, colors.HexColor("#D5E2E7")),
                ("LEFTPADDING", (0, 0), (-1, -1), 10),
                ("RIGHTPADDING", (0, 0), (-1, -1), 10),
                ("TOPPADDING", (0, 0), (-1, -1), 9),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 9),
            ]))
            story.extend([block, Spacer(1, 10)])
        elif token.type in ("bullet_list_open", "ordered_list_open"):
            lists.append(None if token.type == "bullet_list_open" else int(token.attrGet("start") or 1))
        elif token.type == "list_item_open":
            bullet = "•" if lists[-1] is None else f"{lists[-1]}."
            if lists[-1] is not None:
                lists[-1] += 1
        elif token.type in ("bullet_list_close", "ordered_list_close"):
            lists.pop()
            story.append(Spacer(1, 5))
        elif token.type == "blockquote_open":
            quote = True
        elif token.type == "blockquote_close":
            quote = False
        elif token.type == "table_open":
            rows, row = [], []
            while tokens[index].type != "table_close":
                current = tokens[index]
                if current.type == "tr_open":
                    row = []
                elif current.type == "inline":
                    row.append(Paragraph(inline(current), style["CellHead" if not rows else "Cell"]))
                elif current.type == "tr_close":
                    rows.append(row)
                index += 1
            widths = [WIDTH * part for part in (0.22, 0.34, 0.21, 0.23)] if len(rows[0]) == 4 else None
            table = Table(rows, colWidths=widths, repeatRows=1, hAlign="LEFT")
            table.setStyle(TableStyle([
                ("BACKGROUND", (0, 0), (-1, 0), NAVY),
                ("ROWBACKGROUNDS", (0, 1), (-1, -1), [colors.white, PALE]),
                ("VALIGN", (0, 0), (-1, -1), "TOP"),
                ("LEFTPADDING", (0, 0), (-1, -1), 7),
                ("RIGHTPADDING", (0, 0), (-1, -1), 7),
                ("TOPPADDING", (0, 0), (-1, -1), 7),
                ("BOTTOMPADDING", (0, 0), (-1, -1), 7),
                ("LINEBELOW", (0, 0), (-1, 0), 1, TEAL),
            ]))
            story.extend([table, Spacer(1, 10)])
        index += 1
    return story


def main():
    root = Path(__file__).resolve().parent
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--source", type=Path, default=root / "user-guide.md")
    parser.add_argument("--output", type=Path, default=root / "user-guide.pdf")
    parser.add_argument("--date", default=date.today().isoformat())
    options = parser.parse_args()
    version = (root.parent / "VERSION").read_text().strip()
    published = date.fromisoformat(options.date).strftime("%d %B %Y")
    style = styles()
    title = ParagraphStyle("Cover", fontName="Helvetica-Bold", fontSize=34,
                           leading=40, textColor=NAVY, spaceAfter=24)
    story = [Spacer(1, 32 * mm), Paragraph("GBI CLI<br/>&amp; Python", title),
             Paragraph("A practical guide to moving, archiving and restoring research data.",
                       ParagraphStyle("Lead", parent=style["Body"], fontSize=16, leading=23)),
             Spacer(1, 13 * mm), Paragraph("COPY keeps your originals.<br/>"
                 "MOVE verifies before removing eligible sources.<br/>"
                 "RESTORE brings an archive back to files and folders.", style["Callout"]),
             Spacer(1, 10 * mm), Paragraph("Start with a small copy. Check the paths and result. "
                 "Then use the same tools for your finished datasets and checkpoints.", style["Body"]),
             Paragraph(f"User guide for GBI {version}<br/>{published}", style["Body"]), PageBreak(),
             Paragraph("Find what you need", style["Section"])]
    toc = TableOfContents()
    toc.levelStyles = [ParagraphStyle("Contents", fontName="Helvetica", fontSize=11,
                                     leading=19, textColor=NAVY, alignment=TA_LEFT)]
    story.extend([toc, Spacer(1, 12 * mm), Paragraph(
        "Examples use placeholder paths. Run <font name='Courier'>gbi data roots</font> "
        "and substitute your own paths before copying a command. The installed "
        "<font name='Courier'>--help</font> describes the version on your cluster.", style["Callout"]),
        PageBreak()])
    story.extend(content(options.source.read_text(), style))
    options.output.parent.mkdir(parents=True, exist_ok=True)
    Guide(options.output, version, published).multiBuild(story)
    print(options.output)


if __name__ == "__main__":
    main()
