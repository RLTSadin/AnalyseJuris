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
    """Return textual columns to focus keyword filtering on relevant data.

    The script needs to determine which columns of the CSV file contain
    free-form text before it can search for the words contained in a user's
    question. Numeric or categorical columns are rarely useful for this
    purpose because keyword matching on them produces either no results or
    many false positives. This helper therefore scans every column and keeps
    only those whose pandas dtype is recognised as a string-like type (either
    the generic ``object`` dtype or anything reported as ``is_string_dtype``).

    Parameters
    ----------
    df:
        The dataframe loaded from the CSV file.

    Returns
    -------
    list[str]
        The names of the columns that store textual values.
    """
    return [col for col in df.columns if df[col].dtype == object or pd.api.types.is_string_dtype(df[col].dtype)]


def _keyword_set(question: str) -> set[str]:
    """Extract a cleaned set of keywords from the user's question.

    The user may type sentences that include punctuation, short filler words
    or uppercase characters. To make the filtering logic more reliable we
    normalise the question to lowercase, extract alphanumeric tokens and drop
    very short words (of length two or less) that generally correspond to
    stopwords such as "de" or "et" in French. The resulting set is used to
    search the CSV content.

    Parameters
    ----------
    question:
        The raw user prompt entered in the interactive session.

    Returns
    -------
    set[str]
        Unique keywords deemed relevant for matching against CSV rows.
    """
    tokens = re.findall(r"\w+", question.lower())
    return {token for token in tokens if len(token) > 2}


def _candidate_rows(df: pd.DataFrame, question: str, max_rows: int) -> pd.DataFrame:
    """Select rows that are most likely to answer the user's question.

    Because the OpenAI API performs best with concise prompts, we limit the
    number of rows passed to the model. This function first extracts keywords
    from the question, identifies the textual columns where those keywords
    could appear, and builds a boolean mask that tracks rows containing any of
    them. When the question is too vague (no keywords extracted), when the CSV
    lacks textual columns or when the keyword search does not match anything,
    the function falls back to returning the first ``max_rows`` rows to ensure
    the prompt still contains data.

    Parameters
    ----------
    df:
        The dataframe loaded from the CSV file.
    question:
        The natural-language query provided by the user.
    max_rows:
        Maximum number of rows to return in the subset.

    Returns
    -------
    pandas.DataFrame
        A dataframe containing at most ``max_rows`` rows deemed relevant.
    """
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
    """Generate a human-readable description of each column in the dataset.

    To help the language model understand the structure of the CSV file we
    provide a short summary listing every column alongside its pandas data
    type. For additional context we include up to ``sample_size`` non-null
    example values per column. If a column contains only missing values we
    mention that explicitly so the assistant does not assume otherwise.

    Parameters
    ----------
    df:
        The dataframe loaded from the CSV file.
    sample_size:
        Maximum number of example values to display for each column.

    Returns
    -------
    str
        A formatted multi-line description of the dataset's columns.
    """
    parts: list[str] = []
    for column in df.columns:
        dtype = df[column].dtype
        samples = df[column].dropna().astype(str).head(sample_size).tolist()
        sample_text = ", ".join(samples) if samples else "Aucune valeur disponible"
        parts.append(f"- {column} (type {dtype}): {sample_text}")
    return "\n".join(parts)


def build_prompt(csv_path: str, df: pd.DataFrame, question: str, max_rows: int) -> str:
    """Construct the full prompt sent to the language model.

    The prompt must provide enough context for OpenAI's Responses API to give
    a grounded answer. This helper therefore assembles several pieces of
    information: a general description of the CSV (number of rows and
    columns), the column summary, the subset of rows deemed relevant to the
    question and the question itself. The rows are converted back to CSV
    format so that the assistant can read them easily.

    Parameters
    ----------
    csv_path:
        Path to the CSV file, displayed to the user for reference.
    df:
        The dataframe loaded from the CSV file.
    question:
        The user's natural-language question.
    max_rows:
        Maximum number of rows to include in the contextual excerpt.

    Returns
    -------
    str
        A fully assembled textual prompt ready to be sent to the model.
    """
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
    """Send the prompt to OpenAI's Responses API and return the response text.

    We interact with the Responses API through the official SDK. The function
    injects a system message instructing the assistant to behave like a data
    analysis specialist, then forwards the assembled prompt as the user
    message. Only the text content of the assistant's reply is returned to the
    caller, as this is what needs to be printed in the CLI.

    Parameters
    ----------
    client:
        Instantiated OpenAI client used to perform the API call.
    model:
        Identifier of the target model (e.g. ``gpt-4.1-mini``).
    prompt:
        The fully assembled prompt generated by :func:`build_prompt`.

    Returns
    -------
    str
        The assistant's textual answer extracted from the API response.
    """
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
    """Parse command-line arguments for the interactive CLI.

    The script supports specifying the CSV file to analyse, the OpenAI model
    to query and how many rows to include in the prompt. Exposing these
    options through :mod:`argparse` makes the utility flexible while keeping
    the interface self-documented via ``--help``.

    Parameters
    ----------
    args:
        Optional iterable of arguments, primarily used for testing. When
        ``None`` (the default) the arguments are read from :data:`sys.argv`.

    Returns
    -------
    argparse.Namespace
        Namespaced object holding the parsed options.
    """
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
    """Entry point for the command-line interface.

    The function orchestrates the workflow: parse the arguments, make sure the
    API key is available, load the CSV file, then repeatedly read questions
    from the user, build the context and display the model's response. Keyboard
    interrupts or an empty question end the interactive loop gracefully. Any
    unexpected API exception is caught and reported without terminating the
    session so the user can try again.
    """
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
