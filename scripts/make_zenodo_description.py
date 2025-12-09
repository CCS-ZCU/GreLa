#!/usr/bin/env python3
"""
Convert README.md to Zenodo-description-ready HTML.

- Replaces Zenodo badge markdown like
    [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.XXXX.svg)](https://doi.org/10.5281/zenodo.XXXX)

  with a plain clickable DOI link:
    <a href="https://doi.org/10.5281/zenodo.XXXX">DOI: 10.5281/zenodo.XXXX</a>

  (Images are not rendered in Zenodo descriptions.)

- Turns bare URLs into links.
- Preserves nested lists via Python-Markdown's "extra" extension.
"""

import argparse
import re

try:
    import markdown  # pip install markdown
except ImportError as e:
    raise SystemExit(
        "This script requires the 'markdown' package. "
        "Install it with: pip install markdown"
    ) from e


def replace_zenodo_badges_with_text_links(md_text: str) -> str:
    """
    Replace Zenodo badge markdown of the form:

        [![DOI](https://zenodo.org/badge/DOI/10.5281/zenodo.XXXX.svg)](https://doi.org/10.5281/zenodo.XXXX)

    with a plain text DOI link, e.g.:

        [DOI: 10.5281/zenodo.XXXX](https://doi.org/10.5281/zenodo.XXXX)

    (Images are stripped by Zenodo description, but links survive.)
    """

    pattern = re.compile(
        r"\[!\[(?P<alt>[^\]]*)\]\("
        r"(?P<img>https://zenodo\.org/badge[^\)]*)\)\]"
        r"\((?P<href>https?://doi\.org/[^\)]*)\)"
    )

    def repl(match: re.Match) -> str:
        href = match.group("href")
        # Extract DOI part from the URL (everything after doi.org/)
        doi_part = href.split("doi.org/")[-1]
        # Produce simple markdown link; markdown() will turn it into <a>...</a>
        return f"[DOI: {doi_part}]({href})"

    return pattern.sub(repl, md_text)


def linkify_bare_urls(md_text: str) -> str:
    """
    Wrap bare URLs in angle brackets so that Python-Markdown
    turns them into <a href="...">...</a> links.

    Only targets URLs that appear as plain text (not inside
    existing Markdown links).
    """

    url_pattern = re.compile(
        r"(?P<prefix>^|\s)"
        r"(?P<url>https?://[^\s<>()]+)",
        flags=re.MULTILINE,
    )

    def repl(match: re.Match) -> str:
        prefix = match.group("prefix")
        url = match.group("url")
        return f"{prefix}<{url}>"

    return url_pattern.sub(repl, md_text)


def md_to_html(md_text: str) -> str:
    """
    Convert Markdown to HTML suitable for Zenodo description.
    """
    html = markdown.markdown(
        md_text,
        extensions=[
            "extra",        # better lists, etc.
            "tables",
            "fenced_code",
        ],
    )
    return html


def main():
    parser = argparse.ArgumentParser(
        description="Transform README.md into Zenodo-description-ready HTML."
    )
    parser.add_argument(
        "-i",
        "--input",
        default="README.md",
        help="Path to the input README.md file (default: README.md)",
    )
    parser.add_argument(
        "-o",
        "--output",
        default="zenodo_description.html",
        help="Path to the output HTML file (default: zenodo_description.html)",
    )
    args = parser.parse_args()

    # Read Markdown
    with open(args.input, "r", encoding="utf-8") as f:
        md_text = f.read()

    # 1) Replace Zenodo badge markdown with plain DOI links
    md_text = replace_zenodo_badges_with_text_links(md_text)

    # 2) Ensure bare URLs are linkified (e.g. Colab link)
    md_text = linkify_bare_urls(md_text)

    # 3) Convert to HTML
    html = md_to_html(md_text)

    # Write HTML snippet for Zenodo
    with open(args.output, "w", encoding="utf-8") as f:
        f.write(html)

    print(f"Zenodo description HTML written to: {args.output}")


if __name__ == "__main__":
    main()