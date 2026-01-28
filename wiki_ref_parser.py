#!/usr/bin/env python3
# -*- coding: utf-8 -*-

"""
parse_wikipedia_citations.py

Parse Wikipedia-like reference list HTML fragments (e.g., <li id="cite_note-..."> ...)
and extract structured citation data:
- li id (cite_note-xxx)
- citation type (web/news/pressrelease/etc)
- external URL, title, site/journal, date, retrieved/access date
- PDF flag
- COinS (Z3988) span title attribute parsed into key-value (rft.* etc)

Usage:
  python parse_wikipedia_citations.py -i input.html -o out.csv
  python parse_wikipedia_citations.py -i input.html --json out.json
  cat input.html | python parse_wikipedia_citations.py -o out.csv

Dependencies:
  pip install beautifulsoup4 lxml
"""

from __future__ import annotations

import argparse
import csv
import html
import json
import re
import sys
from dataclasses import dataclass, asdict
from datetime import datetime
from typing import Any, Dict, List, Optional, Tuple

from bs4 import BeautifulSoup  # type: ignore


@dataclass
class CitationRecord:
    li_id: str
    citation_classes: str
    citation_kind: str  # web/news/pressrelease/unknown
    title: str
    url: str
    site_or_journal: str
    date_raw: str
    retrieved_raw: str
    is_pdf: bool
    coins_raw: str
    coins: Dict[str, str]
    raw_text: str


def _safe_text(node) -> str:
    if not node:
        return ""
    return " ".join(node.get_text(" ", strip=True).split())


def _first_nonempty(*vals: str) -> str:
    for v in vals:
        if v and v.strip():
            return v.strip()
    return ""


def _is_pdf_from_text_or_url(text: str, url: str) -> bool:
    t = (text or "").lower()
    u = (url or "").lower()
    if "(pdf)" in t:
        return True
    if u.endswith(".pdf"):
        return True
    if ".pdf?" in u or ".pdf#" in u:
        return True
    return False


def _extract_citation_kind(class_list: List[str]) -> str:
    # Wikipedia often uses: citation web cs1 / citation news cs1 / citation pressrelease cs1
    # We'll map known tokens.
    tokens = set(c.lower() for c in class_list)
    if "web" in tokens:
        return "web"
    if "news" in tokens:
        return "news"
    if "pressrelease" in tokens:
        return "pressrelease"
    # Sometimes only "citation" appears; try infer later
    return "unknown"


def _parse_coins_title_attr(coins_title: str) -> Dict[str, str]:
    """
    COinS (Z3988) often stored in a span title attribute:
      ctx_ver=Z39.88-2004&rft_val_fmt=info:ofi/fmt:kev:mtx:journal&rft.genre=unknown&...
    We'll parse into dict. We also HTML-unescape it first.
    """
    if not coins_title:
        return {}
    s = html.unescape(coins_title).strip()
    # Sometimes there are stray leading/trailing quotes or spaces
    s = s.strip().strip('"').strip("'")
    parts = s.split("&")
    out: Dict[str, str] = {}
    for p in parts:
        if not p:
            continue
        if "=" not in p:
            out[p] = ""
            continue
        k, v = p.split("=", 1)
        out[html.unescape(k)] = html.unescape(v)
    return out


def _find_coins_span(li_tag) -> Tuple[str, Dict[str, str]]:
    # COinS span typically: <span class="Z3988" title="ctx_ver=..."></span>
    span = li_tag.find("span", class_="Z3988")
    if not span:
        return "", {}
    title_attr = span.get("title", "") or ""
    title_attr = html.unescape(title_attr)
    return title_attr, _parse_coins_title_attr(title_attr)


def _extract_main_link_and_title(li_tag) -> Tuple[str, str]:
    """
    Strategy:
      - Find <cite class="citation ..."> within li
      - Inside cite, find first <a class="external text" rel="nofollow" ...>
      - Title usually anchor text, but sometimes quoted in text nodes.
    """
    cite = li_tag.find("cite", class_=re.compile(r"\bcitation\b"))
    if not cite:
        # fallback: any external link inside li
        a = li_tag.find("a", class_=re.compile(r"\bexternal\b"))
        url = a.get("href", "") if a else ""
        title = _safe_text(a) if a else ""
        return url, title

    a = cite.find("a", class_=re.compile(r"\bexternal\b"))
    url = a.get("href", "") if a else ""
    title = _safe_text(a) if a else ""

    # Sometimes title might be in quotes in cite text even if anchor exists
    # We'll trust anchor text if non-empty; else use cite text.
    if not title:
        title = _safe_text(cite)

    return url, title


def _extract_site_and_dates(li_tag) -> Tuple[str, str, str]:
    """
    Extract:
      - site/journal from <i> tag inside cite (e.g., <i>www.spice-indices.com</i> or <i>Bloomberg</i>)
      - date (often plain text like 2014-10-29 or "March 29, 2011.")
      - retrieved/access date from <span class="reference-accessdate"> ... Retrieved ... </span>
    """
    cite = li_tag.find("cite", class_=re.compile(r"\bcitation\b"))
    if not cite:
        return "", "", ""

    site = ""
    ital = cite.find("i")
    if ital:
        site = _safe_text(ital)

    # Date: frequently appears as a text node after </i> or before accessdate span
    # We'll take cite text and regex a date-like substring.
    cite_text = _safe_text(cite)

    # Accessdate span
    access_span = cite.find("span", class_=re.compile(r"\breference-accessdate\b"))
    retrieved = _safe_text(access_span)

    # Try to locate a date in cite_text. Examples:
    #   "... . www.spice-indices.com. 2014-09-12 . Retrieved 2014-09-27."
    #   "... . Forbes. March 25, 2011."
    date_raw = ""

    # Prefer ISO date first
    m_iso = re.search(r"\b(19|20)\d{2}-\d{2}-\d{2}\b", cite_text)
    if m_iso:
        date_raw = m_iso.group(0)
    else:
        # Month Day, Year
        m_mdy = re.search(
            r"\b(January|February|March|April|May|June|July|August|September|October|November|December)\s+\d{1,2},\s+(19|20)\d{2}\b",
            cite_text,
            flags=re.IGNORECASE,
        )
        if m_mdy:
            date_raw = m_mdy.group(0)
        else:
            # "24 October 2011" style
            m_dmy = re.search(
                r"\b\d{1,2}\s+(January|February|March|April|May|June|July|August|September|October|November|December)\s+(19|20)\d{2}\b",
                cite_text,
                flags=re.IGNORECASE,
            )
            if m_dmy:
                date_raw = m_dmy.group(0)

    return site, date_raw, retrieved


def parse_citations_from_html(html_text: str) -> List[CitationRecord]:
    soup = BeautifulSoup(html_text, "lxml")

    # Wikipedia reference items usually are <li id="cite_note-xxx"> ... </li>
    li_items = soup.find_all("li", id=re.compile(r"^cite_note-"))
    records: List[CitationRecord] = []

    # If the fragment doesn't include <li>, try to find any <cite class="citation ...">
    if not li_items:
        # Wrap citations into pseudo records
        cites = soup.find_all("cite", class_=re.compile(r"\bcitation\b"))
        for idx, cite in enumerate(cites, start=1):
            pseudo_li_id = f"cite_note-fragment-{idx}"
            # Create a fake li wrapper for uniform parsing
            fake_li = soup.new_tag("li", id=pseudo_li_id)
            fake_li.append(cite)
            li_items.append(fake_li)

    for li in li_items:
        li_id = li.get("id", "") or ""
        cite = li.find("cite", class_=re.compile(r"\bcitation\b"))

        citation_classes = ""
        citation_kind = "unknown"
        if cite:
            cls = cite.get("class", [])
            cls_list = [c for c in cls if isinstance(c, str)]
            citation_classes = " ".join(cls_list)
            citation_kind = _extract_citation_kind(cls_list)

        url, title = _extract_main_link_and_title(li)
        site, date_raw, retrieved = _extract_site_and_dates(li)
        raw_text = _safe_text(li)

        coins_raw, coins_dict = _find_coins_span(li)

        # If citation_kind still unknown, infer from url/title patterns
        if citation_kind == "unknown":
            if "prnewswire" in (url or "").lower():
                citation_kind = "pressrelease"
            elif site.lower() in {"bloomberg", "reuters", "ap", "associated press", "forbes"}:
                citation_kind = "news"
            else:
                citation_kind = "web"

        is_pdf = _is_pdf_from_text_or_url(raw_text, url)

        rec = CitationRecord(
            li_id=li_id,
            citation_classes=citation_classes,
            citation_kind=citation_kind,
            title=title,
            url=url,
            site_or_journal=site,
            date_raw=date_raw,
            retrieved_raw=retrieved,
            is_pdf=is_pdf,
            coins_raw=coins_raw,
            coins=coins_dict,
            raw_text=raw_text,
        )
        records.append(rec)

    return records


def write_csv(records: List[CitationRecord], out_path: str) -> None:
    # Flatten COinS dict into JSON string to keep CSV simple
    fieldnames = [
        "li_id",
        "citation_kind",
        "citation_classes",
        "title",
        "url",
        "site_or_journal",
        "date_raw",
        "retrieved_raw",
        "is_pdf",
        "coins_raw",
        "coins_json",
        "raw_text",
    ]
    with (open(out_path, "w", newline="", encoding="utf-8") if out_path != "-" else sys.stdout) as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for r in records:
            row = {
                "li_id": r.li_id,
                "citation_kind": r.citation_kind,
                "citation_classes": r.citation_classes,
                "title": r.title,
                "url": r.url,
                "site_or_journal": r.site_or_journal,
                "date_raw": r.date_raw,
                "retrieved_raw": r.retrieved_raw,
                "is_pdf": str(bool(r.is_pdf)),
                "coins_raw": r.coins_raw,
                "coins_json": json.dumps(r.coins, ensure_ascii=False),
                "raw_text": r.raw_text,
            }
            writer.writerow(row)


def write_json(records: List[CitationRecord], out_path: str) -> None:
    data = []
    for r in records:
        d = asdict(r)
        data.append(d)

    if out_path == "-":
        sys.stdout.write(json.dumps(data, ensure_ascii=False, indent=2))
        sys.stdout.write("\n")
        return

    with open(out_path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def main() -> int:
    ap = argparse.ArgumentParser(
        description="Parse Wikipedia reference HTML fragments into structured citation data."
    )
    ap.add_argument(
        "-i",
        "--input",
        default="",
        help="Input HTML file path. If omitted or '-', read from stdin.",
    )
    ap.add_argument(
        "-o",
        "--output",
        default="-",
        help="Output CSV path. Use '-' for stdout. Default: stdout.",
    )
    ap.add_argument(
        "--json",
        default="",
        help="Also write JSON output to this path (use '-' for stdout).",
    )
    ap.add_argument(
        "--min",
        type=int,
        default=0,
        help="Minimum number of records required; exit non-zero if fewer.",
    )
    args = ap.parse_args()

    if args.input and args.input != "-":
        with open(args.input, "r", encoding="utf-8") as f:
            html_text = f.read()
    else:
        html_text = sys.stdin.read()

    # Unescape once to convert &#95; etc in attribute chunks if present
    html_text = html.unescape(html_text)

    records = parse_citations_from_html(html_text)

    if args.min and len(records) < args.min:
        sys.stderr.write(f"[ERROR] Only {len(records)} records parsed; expected >= {args.min}\n")
        return 2

    # Always write CSV
    write_csv(records, args.output)

    # Optional JSON
    if args.json:
        write_json(records, args.json)

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
