# stringest

`stringest` (str-ingest) is intended to be a tool to interpret inbound string data. It takes data
in string format (i.e. CSV, JSONL) and does some validation, transformation, and parsing. The end
result should be a correctly-typed parquet file and a collection of line-numbered (or file level)
validation messages with different statuses.

## Why use `stringest`?

`stringest` should be easy to set up and run, reasonably performant and configurable. It should
support effective logging and produce sensible, inteligible error messages for ingestion failures.
It should be kind to be kind to memory, using iteration and streaming to work on large files.

Where other libraries focus on ensuring that parsed Python data structures match specific,
machine-readable schemas, `stringest` aims to ensure that received text data can be coerced
into a human-readable, machine-interpretable schema. `stringest` schemas will be able to be
serialised and deserialised into JSON, but will also support rendering as readable markdown
documents.
