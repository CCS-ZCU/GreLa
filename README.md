# GreLa ETL

---
## Authors
* Vojtěch Kaše (with team of collaborators)

## License
CC-BY-SA 4.0 — see the attached `License.md`.

---
## Description

This repository contains the code for creating, maintaining, and enriching the **GreLa corpus**.

**GreLa** is a comprehensive corpus of Greek and Latin literature from the 8th c. BCE to the 17th c. CE.  
It currently contains more than **11,000 works**, **21,000,000 sentences**, and **350,000,000 tokens**.

GreLa is formed as a merge of the following corpora:

* **[LAGT](https://zenodo.org/records/13889714)** — Lemmatized Ancient Greek Texts, combining ancient Greek texts from the Perseus Digital Library, First 1,000 Years of Greek, Glaux, and OGA.
* **[Corpus Corporum](https://mlat.uzh.ch)** — a comprehensive corpus of Latin literature.
* **[NOSCEMUS](https://zenodo.org/records/15040256)** — a curated database of Early Modern scientific literature.
* **[EMLAP](https://zenodo.org/records/14765511)** — Early Modern Latin Alchemical Prints.
* **[latin-lemmatized-texts](https://github.com/lascivaroma/latin-lemmatized-texts/tree/main)** — used here as a source for the lemmatized Vulgate.

### Corpus statistics

| grela_source   | works_N | sentences_N | tokens_N    |
|:---------------|--------:|------------:|------------:|
| lagt           | 2,160   | 2,095,265   | 38,223,149  |
| cc             | 7,819   | 14,229,691  | 254,770,887 |
| noscemus       |   975   | 4,637,231   | 54,542,448  |
| emlap          |   100   |   411,638   |  6,385,345  |
| vulgate        |    73   |    35,254   |    603,091  |

GreLa is implemented as a relational database with three main tables: **`works`**, **`sentences`**, and **`tokens`**.  
The schema links tables through:

- **`grela_id`** — unique ID for each work (built as `<subcorpus>_<work-id>`, e.g., `cc_12710`)  
- **`sentence_id`** — unique ID for each sentence (`<grela_id>_<position>`, e.g., `cc_12710_0`, `cc_12710_1`)

### Querying the corpus

The **tokens** table allows searching by lemma, POS, and positional information (`char_start`, `char_end`).  
Where available, the `ref` JSON attribute encodes textual reference metadata (such as book/chapter/verse for biblical or structured texts). This varies significantly across subcorpora.

The **sentences** table supports efficient search for multi-word string patterns in raw text.

The **works** table contains rich metadata for each work. The fields `not_before` and `not_after` express a chronological interval. Ancient texts often require such interval dating, and GreLa supports temporal uncertainty using Monte Carlo modeling as described in [this paper](https://ceur-ws.org/Vol-3558/paper5123.pdf).  
Following this method, each work is also assigned a **`date_random`** point estimate sampled from its interval.

Additionally, the works table provides identifiers such as:

- `author_viaf`
- `author_wd` (Wikidata QID)
- `author_gnd`

as well as subcorpus-specific metadata stored uniformly in the `subcorpus_specific_metadata` JSON field.

GreLa uses **DuckDB**, an efficient column-oriented analytical database engine optimized for complex queries over large datasets.

---

# Database Schema Documentation

## Table: `sentences`

| Column Name     | Data Type    | Is Nullable | Default Value |
|-----------------|-------------|-------------|---------------|
| sentence_id | VARCHAR | YES | N/A |
| grela_id | VARCHAR | YES | N/A |
| position | INTEGER | YES | N/A |
| sent_text | VARCHAR | YES | N/A |

## Table: `tokens`

| Column Name     | Data Type    | Is Nullable | Default Value |
|-----------------|-------------|-------------|---------------|
| sentence_id | VARCHAR | YES | N/A |
| grela_id | VARCHAR | YES | N/A |
| token_text | VARCHAR | YES | N/A |
| lemma | VARCHAR | YES | N/A |
| pos | VARCHAR | YES | N/A |
| ref | JSON | YES | N/A |
| char_start | INTEGER | YES | N/A |
| char_end | INTEGER | YES | N/A |
| token_id | BIGINT | YES | N/A |

## Table: `works`

| Column Name     | Data Type    | Is Nullable | Default Value |
|-----------------|-------------|-------------|---------------|
| grela_source | VARCHAR | YES | N/A |
| grela_id | VARCHAR | YES | N/A |
| author | VARCHAR | YES | N/A |
| title | VARCHAR | YES | N/A |
| not_before | INTEGER | YES | N/A |
| not_after | INTEGER | YES | N/A |
| date_random | INTEGER | YES | N/A |
| provenience | VARCHAR | YES | N/A |
| place_publication | VARCHAR | YES | N/A |
| place_geonames | VARCHAR | YES | N/A |
| author_viaf | VARCHAR | YES | N/A |
| author_wd | VARCHAR | YES | N/A |
| author_gnd | VARCHAR | YES | N/A |
| title_viaf | VARCHAR | YES | N/A |
| subcorpus_specific_metadata | JSON | YES | N/A |

## Table: `works_df`

| Column Name     | Data Type    | Is Nullable | Default Value |
|-----------------|-------------|-------------|---------------|
| grela_source | VARCHAR | YES | N/A |
| grela_id | VARCHAR | YES | N/A |
| author | VARCHAR | YES | N/A |
| title | VARCHAR | YES | N/A |
| not_before | DOUBLE | YES | N/A |
| not_after | DOUBLE | YES | N/A |
| date_random | DOUBLE | YES | N/A |
| provenience | VARCHAR | YES | N/A |
| place_publication | VARCHAR | YES | N/A |
| place_geonames | VARCHAR | YES | N/A |
| author_viaf | VARCHAR | YES | N/A |
| author_wd | VARCHAR | YES | N/A |
| author_gnd | VARCHAR | YES | N/A |
| title_viaf | DOUBLE | YES | N/A |
| subcorpus_specific_metadata | STRUCT(lagt_tlg_epithet VARCHAR, lagt_genre VARCHAR, noscemus_place VARCHAR, noscemus_genre VARCHAR, noscemus_discipline VARCHAR, emlap_noscemus_id DOUBLE) | YES | N/A |

---

## Getting Started

GreLa is accessible via a public web API.  
To get started, check the introductory Colab notebook:

👉 https://colab.research.google.com/github/CCS-ZCU/GreLa/blob/master/scripts/GreLa-API_getting-started.ipynb

---

## Version History

* **0.6**
  * input data in unified format  
  * EMLAP extended to all 100 works  
  * CC input derived from Lemmatized XML with `ref` metadata  
  * `works` table enriched with VIAF, Wikidata ID, GND  
  * subcorpus-specific attributes unified into `subcorpus_specific_metadata`

* **0.5**
  * various minor improvements

* **0.4**
  * significantly improved Greek sentence and token segmentation  
  * added `ref` attribute for Greek works

* **0.1**
  * first version of GreLa

---

## Roadmap

* `ref` attribute documentation
* add collaborators as coauthors based on agreement
* document licences for all source corpora
* ore identifiers for works and authors (e.g. PHI IDs for Latin texts)  
* provenance metadata for Latin texts  
* standardized spatial metadata for works and authors  

---

## How to Cite

*(After publishing a Zenodo release, place the official citation here.)*

---

## Acknowledgement

*(Add funding and collaboration acknowledgements here.)*