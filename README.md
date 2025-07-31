#  GreLa ETL

---
## Authors
* Vojtěch Kaše (& team of collaborators)


## License
CC-BY-SA 4.0, see attached License.md

---
## Description

This repository serves for the creation, maintenance, and enrichment of the GreLa corpus

GreLa is a comprehensive corpus of Greek and Latin literature from the 8 c. BCE to the 17. c. CE. It covers more than 11,000 works, 26,000,000 sentences and 380,000,000 tokens. It is formed as a merge of the following corpora:
* [LAGT](https://zenodo.org/records/13889714): Lemmatized Ancient Greek Texts, combining all ancient Greek texts from Perseus Digital Library, First 1,000 Years of Greek, Glaux and OGA.
* [Corpus Corporum](https://mlat.uzh.ch): a comprehensive corpus of Latin literature
* [NOSCEMUS](https://zenodo.org/records/15040256): a database of early Modern scientific literature
* [EMLAP](https://zenodo.org/records/14765511): Early Modern Latin Alchemical Prints

| subcorpus   | works_N   | sentences_N   | tokens_N    |
|:------------|:----------|:--------------|:------------|
| cc          | 7,819     | 11,835,457    | 201,939,293 |
| emlap       | 73        | 220,846       | 3,495,212   |
| lagt        | 1,957     | 2,703,678     | 35,808,742  |
| noscemus    | 996       | 11,802,783    | 139,401,899 |
| vulgate     | 73        | 35,254        | 603,091     |


GreLa is structured as a relational database currently consisting of three tables: **works**, **sentences**, and **tokens**. The tables are mapped on each other using the keys `grela_id` and `sentence_id`. `grela_id` is formed as a combination of the subcorpus akronym and the ID of the work in the respective subcorpus (`<subcorpus-akronym>_<work-id>`, e.g. `cc_1271O`). `sentence_id` extends `grela_id` by positional index of the sentence, starting from 0 (e.g. `cc_12710_0` and `cc_12710_1` stand for the first two sentences from the work with the ID 12710 in *Corpus Corporum*).

In the **tokens** table, you can, for instance, search using the `lemma`  and `pos_tag` fields. You can also retrieve the position of the token within the respective sentence using `char_start` and `char_end`. 

In the **works** table, we are gradually adding additional metadata for individual works. Most importantly, we offer a date using the fields `not_before` and `not_after`. While for early modern works these two attributes are often the same, as the date of publication is known, for works from antiquity, we often have only a rough estimate, which can only be expressed by means of an interval. This dating convention invites a Monte Carlo approach to modeling temporal uncertainty, which we proposed in [this paper](https://ceur-ws.org/Vol-3558/paper5123.pdf).

The database is implemented using DuckDB, an open-source column-oriented Relational Database Management System (RDBMS) designed to provide high performance on complex queries against large databases.

## Database Schema Documentation

### Table: `sentences`

| Column Name     | Data Type    | Is Nullable | Default Value |
|-----------------|-------------|-------------|---------------|
| sentence_id | VARCHAR | YES | N/A |
| grela_id | VARCHAR | YES | N/A |
| position | INTEGER | YES | N/A |
| text | VARCHAR | YES | N/A |

### Table: `tokens`

| Column Name     | Data Type    | Is Nullable | Default Value |
|-----------------|-------------|-------------|---------------|
| sentence_id | VARCHAR | YES | N/A |
| grela_id | VARCHAR | YES | N/A |
| token_text | VARCHAR | YES | N/A |
| lemma | VARCHAR | YES | N/A |
| pos | VARCHAR | YES | N/A |
| char_start | INTEGER | YES | N/A |
| char_end | INTEGER | YES | N/A |
| token_id | BIGINT | YES | N/A |

### Table: `works`

| Column Name     | Data Type    | Is Nullable | Default Value |
|-----------------|-------------|-------------|---------------|
| grela_source | VARCHAR | YES | N/A |
| grela_id | VARCHAR | YES | N/A |
| author | VARCHAR | YES | N/A |
| title | VARCHAR | YES | N/A |
| not_before | DOUBLE | YES | N/A |
| not_after | DOUBLE | YES | N/A |
| lagt_tlg_epithet | VARCHAR | YES | N/A |
| lagt_genre | VARCHAR | YES | N/A |
| lagt_provenience | VARCHAR | YES | N/A |
| noscemus_place | VARCHAR | YES | N/A |
| noscemus_genre | VARCHAR | YES | N/A |
| noscemus_discipline | VARCHAR | YES | N/A |
| title_short | VARCHAR | YES | N/A |
| emlap_noscemus_id | DOUBLE | YES | N/A |
| place_publication | VARCHAR | YES | N/A |
| place_geonames | VARCHAR | YES | N/A |
| author_viaf | DOUBLE | YES | N/A |
| title_viaf | DOUBLE | YES | N/A |
| date_random | DOUBLE | YES | N/A |



## Getting started

GreLa is now accessible via an API. To get started, check [this](https://colab.research.google.com/github/CCS-ZCU/GreLa/blob/master/scripts/GreLa-API_getting-started.ipynb) Google Colab notebook.

```python
# currently, we maintain the database on our CCS-Lab server



```

## How to cite

[once a release is created and published via zenodo, put its citation here]

## Ackwnowledgement

[This work has been supported by ...]
