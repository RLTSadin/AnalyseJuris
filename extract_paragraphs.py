# -*- coding: utf-8 -*-
"""
Extraction des discussions par paragraphe à partir d'un fichier .docx.
Produit un fichier Excel avec les colonnes :
Paragraphe | Page | Référence | Interlocuteur détecté | Interlocuteur courant | Fonction | Organisation | Texte.

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

AffiliationMap = Dict[str, Dict[str, str]]

# ========= À PERSONNALISER =========
IN_DOCX = Path(
    r"C:/Users/samue/Documents/Doctorat/Contrats/Laura/Services de garde et RI-RTF/PL51/Compilation_Loi_51.docx"
)
OUT_XLSX = Path(
    r"C:/Users/samue/Documents/Doctorat/Contrats/Laura/Services de garde et RI-RTF/PL51/Analyse_par_paragraphes.xlsx"
)

INTERVENANTS_XLSX = Path(
    r"C:/Users/samue/Documents/Doctorat/Contrats/Laura/Services de garde et RI-RTF/Tableaux/Répertoire_Intervenants.xlsx"
)

INTERVENANT_NOM_COL = "intervenant"
INTERVENANT_FONCTION_COL = "fonction"
INTERVENANT_ORGANISATION_COL = "organisation"

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


def normalize_speaker_key(value: str) -> str:
    cleaned = normalize_text(value)
    cleaned = strip_accents(cleaned).lower()
    cleaned = re.sub(r"[^a-z0-9]+", " ", cleaned)
    return re.sub(r"\s+", " ", cleaned).strip()


def resolve_column_name(columns: List[str], target: str) -> str:
    target_lower = target.lower()
    for column in columns:
        if column.lower() == target_lower:
            return column
    raise KeyError(f"Colonne '{target}' introuvable dans le fichier des intervenants")


def load_affiliations(xlsx_path: Path) -> AffiliationMap:
    if not xlsx_path.exists():
        return {}

    dataframe = pd.read_excel(xlsx_path)
    if dataframe.empty:
        return {}

    name_col = resolve_column_name(list(dataframe.columns), INTERVENANT_NOM_COL)
    fonction_col = resolve_column_name(list(dataframe.columns), INTERVENANT_FONCTION_COL)
    organisation_col = resolve_column_name(list(dataframe.columns), INTERVENANT_ORGANISATION_COL)

    mapping: AffiliationMap = {}
    for _, row in dataframe.iterrows():
        raw_name = row.get(name_col, "")
        if pd.isna(raw_name):
            continue

        key = normalize_speaker_key(str(raw_name))
        if not key:
            continue

        fonction_value = row.get(fonction_col, "")
        organisation_value = row.get(organisation_col, "")

        fonction = "" if pd.isna(fonction_value) else str(fonction_value).strip()
        organisation = "" if pd.isna(organisation_value) else str(organisation_value).strip()

        mapping[key] = {"fonction": fonction, "organisation": organisation}

    return mapping


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

        segments = []
        for run in element.findall(".//w:r", ns):
            texts = [node.text for node in run.findall(".//w:t", ns) if node.text]
            if not texts:
                continue

            run_text = "".join(texts)
            run_props = run.find("w:rPr", ns)
            is_bold = False
            if run_props is not None:
                is_bold = run_props.find("w:b", ns) is not None or run_props.find("w:bCs", ns) is not None

            segments.append({"text": run_text, "bold": is_bold})

        full_text = "".join(segment["text"] for segment in segments)
        if not full_text.strip():
            continue

        runs: List[Dict[str, int | str]] = []
        cursor = 0
        for segment in segments:
            text = segment["text"]
            runs.append(
                {
                    "text": text,
                    "start": cursor,
                    "end": cursor + len(text),
                    "bold": bool(segment["bold"]),
                }
            )
            cursor += len(text)

        speaker = detect_speaker(full_text, runs)

        paragraphs.append(
            {
                "index": paragraph_index,
                "page": page_number,
                "text": full_text,
                "speaker": speaker,
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


def detect_speaker(text: str, runs: List[Dict[str, int | str]]) -> str | None:
    """Identifie l'interlocuteur si le nom est en gras et suivi d'un deux-points."""

    colon_index = text.find(":")
    if colon_index == -1 or colon_index > 120:
        return None

    # Ignore les espaces en début de paragraphe
    prefix_start = None
    for idx, char in enumerate(text[:colon_index]):
        if not char.isspace():
            prefix_start = idx
            break

    if prefix_start is None or prefix_start >= colon_index:
        return None

    prefix_end = colon_index

    # Vérifie que toutes les lettres (hors espaces) avant le deux-points sont en gras
    for position in range(prefix_start, prefix_end):
        character = text[position]
        if character.isspace():
            continue

        run = next((item for item in runs if item["start"] <= position < item["end"]), None)
        if run is None or not run["bold"]:
            return None

    candidate = normalize_text(text[prefix_start:prefix_end])
    return candidate or None


def build_paragraph_dataframe(docx_path: Path) -> pd.DataFrame:
    paragraphs = load_docx_paragraphs(docx_path)
    rows: List[Dict[str, int | str]] = []
    current_articles: List[str] = []
    current_speaker: str = ""
    affiliations = load_affiliations(INTERVENANTS_XLSX)

    for paragraph in paragraphs:
        text = normalize_text(paragraph["text"])
        mentions = extract_article_mentions(text)
        adoption = contains_adoption_marker(text)
        detected_speaker = paragraph.get("speaker")

        if detected_speaker:
            current_speaker = normalize_text(detected_speaker)
        detected_value = normalize_text(detected_speaker) if detected_speaker else ""

        fonction = ""
        organisation = ""
        if current_speaker:
            affiliation_key = normalize_speaker_key(current_speaker)
            affiliation = affiliations.get(affiliation_key)
            if affiliation:
                fonction = affiliation.get("fonction", "")
                organisation = affiliation.get("organisation", "")

        if mentions:
            mentions = sorted(mentions, key=sort_article_key)
            current_articles = mentions

        if current_articles:
            rows.append(
                {
                    "Paragraphe": paragraph["index"] + 1,
                    "Page": paragraph["page"],
                    "Référence": ", ".join(current_articles),
                    "Interlocuteur détecté": detected_value,
                    "Interlocuteur courant": current_speaker,
                    "Fonction": fonction,
                    "Organisation": organisation,
                    "Texte": text,
                }
            )

        if adoption:
            current_articles = []

    return pd.DataFrame(
        rows,
        columns=[
            "Paragraphe",
            "Page",
            "Référence",
            "Interlocuteur détecté",
            "Interlocuteur courant",
            "Fonction",
            "Organisation",
            "Texte",
        ],
    )


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
        worksheet.set_column(3, 4, 28)
        worksheet.set_column(5, 6, 28)
        worksheet.set_column(7, 7, 120)

    print(f"✅ Fichier écrit : {OUT_XLSX}")


if __name__ == "__main__":
    main()
