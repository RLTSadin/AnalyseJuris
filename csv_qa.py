"""Interactive CSV question-answering assistant using OpenAI's API."""
from __future__ import annotations

import argparse
import os
import re
from collections.abc import Sequence
from typing import Iterable

import pandas as pd
from openai import OpenAI


def _string_columns(df: pd.DataFrame) -> Sequence[str]:
    """Return the columns that are likely to contain free-form text."""
    return [col for col in df.columns if df[col].dtype == object or pd.api.types.is_string_dtype(df[col].dtype)]


def _keyword_set(question: str) -> set[str]:
    """Extract a set of lowercase keywords from the user's question."""
    tokens = re.findall(r"\w+", question.lower())
    return {token for token in tokens if len(token) > 2}


def _candidate_rows(df: pd.DataFrame, question: str, max_rows: int) -> pd.DataFrame:
    """Select rows that are likely to be relevant for the given question."""
    keywords = _keyword_set(question)
    if not keywords:
        return df.head(max_rows)

    text_columns = _string_columns(df)
    if not text_columns:
        return df.head(max_rows)

    mask = pd.Series(False, index=df.index)
    for word in keywords:
        for column in text_columns:
            column_text = df[column].astype(str)
            mask |= column_text.str.contains(word, case=False, na=False)
    filtered = df[mask]
    if filtered.empty:
        return df.head(max_rows)
    return filtered.head(max_rows)


def _summarise_columns(df: pd.DataFrame, sample_size: int = 3) -> str:
    """Summarise the columns with their data types and small samples."""
    parts: list[str] = []
    for column in df.columns:
        dtype = df[column].dtype
        samples = df[column].dropna().astype(str).head(sample_size).tolist()
        sample_text = ", ".join(samples) if samples else "Aucune valeur disponible"
        parts.append(f"- {column} (type {dtype}): {sample_text}")
    return "\n".join(parts)


def build_prompt(csv_path: str, df: pd.DataFrame, question: str, max_rows: int) -> str:
    """Construct the contextual prompt for the language model."""
    context_lines = [
        f"Le fichier CSV '{csv_path}' contient {len(df)} lignes et {len(df.columns)} colonnes.",
        "Voici un résumé des colonnes :",
        _summarise_columns(df),
        "",
    ]

    candidate_rows = _candidate_rows(df, question, max_rows)
    context_lines.append(
        "Extrait de lignes pertinentes (limité à {max_rows}) :".format(max_rows=max_rows)
    )
    context_lines.append(candidate_rows.to_csv(index=False))
    context_lines.append("")
    context_lines.append("Question de l'utilisateur :")
    context_lines.append(question)

    return "\n".join(context_lines)


def ask_model(client: OpenAI, model: str, prompt: str) -> str:
    """Send the prompt to OpenAI's Responses API and return the assistant's reply."""
    response = client.responses.create(
        model=model,
        input=[
            {
                "role": "system",
                "content": "Tu es un assistant spécialisé dans l'analyse de jeux de données. Réponds de manière concise et justifie les calculs effectués à partir du contexte fourni.",
            },
            {"role": "user", "content": prompt},
        ],
    )
    return response.output_text


def parse_args(args: Iterable[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Poser des questions à propos d'un fichier CSV en utilisant l'API OpenAI.",
    )
    parser.add_argument("csv_path", help="Chemin vers le fichier CSV à analyser")
    parser.add_argument(
        "--model",
        default="gpt-4.1-mini",
        help="Nom du modèle OpenAI à utiliser (par défaut: gpt-4.1-mini)",
    )
    parser.add_argument(
        "--max-rows",
        type=int,
        default=20,
        help="Nombre maximum de lignes à inclure dans le contexte envoyé au modèle",
    )
    return parser.parse_args(args)


def main() -> None:
    args = parse_args()

    if "OPENAI_API_KEY" not in os.environ:
        raise SystemExit(
            "La variable d'environnement OPENAI_API_KEY doit être définie pour utiliser ce script."
        )

    df = pd.read_csv(args.csv_path)

    print(
        f"Fichier chargé: {args.csv_path} | {len(df)} lignes x {len(df.columns)} colonnes."
    )
    print("Saisissez vos questions (laisser vide pour quitter).\n")

    client = OpenAI()

    while True:
        try:
            question = input("> ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\nFin de la session.")
            break

        if not question:
            print("Fin de la session.")
            break

        prompt = build_prompt(args.csv_path, df, question, args.max_rows)
        try:
            answer = ask_model(client, args.model, prompt)
        except Exception as exc:  # pragma: no cover - defensive logging
            print(f"Erreur lors de l'appel à l'API OpenAI: {exc}")
            continue

        print(f"\n{answer}\n")


if __name__ == "__main__":
    main()
