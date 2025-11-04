# -*- coding: utf-8 -*-
"""
Extraction des discussions par paragraphe à partir d'un fichier .docx.
Produit un fichier Excel avec les colonnes : Paragraphe | Page | Référence | Texte.

Dépendances : pandas, xlsxwriter
"""

from __future__ import annotations

import re
import unicodedata
import xml.etree.ElementTree as ET
import zipfile
from pathlib import Path
from typing import Dict, List

import pandas as pd

# ========= À PERSONNALISER =========
IN_DOCX = Path(
    r"C:/Users/samue/Documents/Doctorat/Contrats/Laura/Services de garde et RI-RTF/PL51/Compilation_Loi_51.docx"
)
OUT_XLSX = Path(
    r"C:/Users/samue/Documents/Doctorat/Contrats/Laura/Services de garde et RI-RTF/PL51/Analyse_par_paragraphes.xlsx"
)

# Termes déclencheurs configurables pour identifier un renvoi à un article de loi
ARTICLE_KEYWORDS = [
    r"art(?:\.|icle)?s?",
    r"alinéa(?:s)?",
]

# Termes qui indiquent la fin d'une discussion (adoption, etc.)
ADOPTION_TERMS = ["adopte"]
# ===================================


def build_article_pattern(keywords: List[str]) -> re.Pattern[str]:
    """Construit le motif regex qui capture une liste de numéros d'article."""

    if not keywords:
        raise ValueError("ARTICLE_KEYWORDS ne peut pas être vide")

    keyword_group = "|".join(keywords)
    art_prefix = rf"(?:l[’\']\s*)?(?:{keyword_group})"
    num = r"\d+(?:\.\d+)*"
    sep = r"(?:,|\set\s|\sou\s)"
    range_pat = rf"(?P<start>{num})\s*(?:[-–à]\s*)(?P<end>{num})"
    single = rf"(?:{num})"

    pattern = re.compile(
        rf"\b{art_prefix}\s+(?P<list>{range_pat}|{single}(?:\s*(?:{sep})\s*{single})*)",
        flags=re.IGNORECASE,
    )
    return pattern


ARTICLE_PATTERN = build_article_pattern(ARTICLE_KEYWORDS)


def normalize_text(value: str) -> str:
    value = value.replace("\u00a0", " ").replace("\t", " ")
    value = re.sub(r"\s+", " ", value)
    return value.strip()


def strip_accents(value: str) -> str:
    return "".join(
        char
        for char in unicodedata.normalize("NFD", value)
        if unicodedata.category(char) != "Mn"
    )


def expand_range(start: str, end: str) -> List[str]:
    """Développe les plages d'entiers (ex.: 3-5 -> [3, 4, 5])."""

    try:
        start_int = int(float(start))
        end_int = int(float(end))
    except Exception:
        return [start, end]

    if start_int <= end_int and "." not in start and "." not in end:
        return [str(number) for number in range(start_int, end_int + 1)]

    return [start, end]


def extract_article_mentions(text: str) -> List[str]:
    """Retourne la liste des numéros d'articles mentionnés dans le texte."""

    found = []
    for match in ARTICLE_PATTERN.finditer(text):
        raw_list = match.group("list")
        range_match = re.search(r"(?P<start>\d+(?:\.\d+)*)\s*(?:[-–à]\s*)(?P<end>\d+(?:\.\d+)*)", raw_list)
        if range_match:
            start, end = range_match.group("start"), range_match.group("end")
            found.extend(expand_range(start, end))
            continue

        tokens = re.split(r"(?i)(?:,|\bet\b|\bou\b)", raw_list)
        for token in tokens:
            cleaned = normalize_text(token)
            if re.fullmatch(r"\d+(?:\.\d+)*", cleaned):
                found.append(cleaned)

    # Déduplique en conservant l'ordre
    seen = set()
    ordered = []
    for number in found:
        if number not in seen:
            ordered.append(number)
            seen.add(number)
    return ordered


def load_docx_paragraphs(docx_path: Path) -> List[Dict[str, int | str]]:
    """Retourne les paragraphes (index, texte, page) dans l'ordre du document."""

    with zipfile.ZipFile(docx_path) as archive:
        xml_bytes = archive.read("word/document.xml")

    ns = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    root = ET.fromstring(xml_bytes)
    body = root.find("w:body", ns)
    if body is None:
        return []

    def qn(tag: str) -> str:
        return f"{{{ns['w']}}}{tag}"

    paragraphs: List[Dict[str, int | str]] = []
    page_number = 1
    paragraph_index = 0

    for element in body.findall(".//w:p", ns):
        has_page_break = False
        if element.find(".//w:lastRenderedPageBreak", ns) is not None:
            has_page_break = True
        else:
            for br in element.findall(".//w:br", ns):
                if br.get(qn("type")) == "page":
                    has_page_break = True
                    break

        if has_page_break and paragraphs:
            page_number += 1
        elif has_page_break and not paragraphs:
            # Si le premier paragraphe contient un saut de page, on considère que la numérotation commence à 1
            page_number = max(page_number, 1)

        runs = [node.text for node in element.findall(".//w:t", ns) if node.text]
        raw_text = "".join(runs).strip()
        if not raw_text:
            continue

        paragraphs.append(
            {
                "index": paragraph_index,
                "page": page_number,
                "text": raw_text,
            }
        )
        paragraph_index += 1

    return paragraphs


def sort_article_key(value: str) -> List[int]:
    try:
        return [int(part) for part in value.split(".")]
    except Exception:
        return [999_999]


def contains_adoption_marker(text: str) -> bool:
    normalized = strip_accents(text).lower()
    return any(term in normalized for term in ADOPTION_TERMS)


def build_paragraph_dataframe(docx_path: Path) -> pd.DataFrame:
    paragraphs = load_docx_paragraphs(docx_path)
    rows: List[Dict[str, int | str]] = []
    current_articles: List[str] = []

    for paragraph in paragraphs:
        text = normalize_text(paragraph["text"])
        mentions = extract_article_mentions(text)
        adoption = contains_adoption_marker(text)

        if mentions:
            mentions = sorted(mentions, key=sort_article_key)
            current_articles = mentions

        if current_articles:
            rows.append(
                {
                    "Paragraphe": paragraph["index"] + 1,
                    "Page": paragraph["page"],
                    "Référence": ", ".join(current_articles),
                    "Texte": text,
                }
            )

        if adoption:
            current_articles = []

    return pd.DataFrame(rows, columns=["Paragraphe", "Page", "Référence", "Texte"])


def main() -> None:
    if not IN_DOCX.exists():
        raise FileNotFoundError(f"Fichier introuvable : {IN_DOCX}")

    dataframe = build_paragraph_dataframe(IN_DOCX)
    OUT_XLSX.parent.mkdir(parents=True, exist_ok=True)

    with pd.ExcelWriter(OUT_XLSX, engine="xlsxwriter") as writer:
        dataframe.to_excel(writer, index=False, sheet_name="Paragraphes")
        worksheet = writer.sheets["Paragraphes"]
        worksheet.set_column(0, 0, 12)
        worksheet.set_column(1, 1, 8)
        worksheet.set_column(2, 2, 20)
        worksheet.set_column(3, 3, 120)

    print(f"✅ Fichier écrit : {OUT_XLSX}")


if __name__ == "__main__":
    main()
