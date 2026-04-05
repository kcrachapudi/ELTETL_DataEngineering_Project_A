# every parser looks like this — same contract
class EDIParser:
    def parse(self, source) -> pd.DataFrame: ...

class HL7Parser:
    def parse(self, source) -> pd.DataFrame: ...

class JSONParser:
    def parse(self, source) -> pd.DataFrame: ...
```

The rest of the pipeline — transform, load, dbt, Airflow, BigQuery — calls `.parse()` and gets a DataFrame back. It has no idea whether the source was EDI, HL7, JSON or smoke signals. It doesn't need to know.

**Adding a new format in the future is then literally one file.** Create `new_format_parser.py`, implement `.parse()`, register it. Done. Nothing else changes.

This is actually the same thing you did 10 years ago with your C# plugins — you just didn't have the vocabulary for it. It's called the **Strategy Pattern** in software engineering. In data engineering circles they call it a **connector architecture** — which is exactly how Airbyte is built under the hood. Every Airbyte connector is just a plugin that returns normalised records. You independently arrived at the same design.

So here's the updated Project 1 structure we'll build:
```
project1/
├── parsers/
│   ├── base_parser.py       ← abstract interface
│   ├── json_parser.py       ← REST API / JSON
│   ├── csv_parser.py        ← flat files
│   ├── edi_parser.py        ← EDI X12 (pyx12)
│   ├── hl7_parser.py        ← HL7 (hl7apy)
│   └── xml_parser.py        ← XML / SOAP
├── extractors/              ← knows WHERE to get data
├── loaders/                 ← knows HOW to write to Postgres
├── config/
├── logs/
└── run.sh