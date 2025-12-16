Unified Outreach Data Pipeline

Hi im Ashwin. This is a lightweight data pipeline that ingests outreach and reply events from multiple platforms, normalizes them into a unified contact schema, and exports a clean, deduplicated CSV.

How to Run
python app.py


This reads events from mock/, merges them into a local SQLite database, and outputs unified.csv.

Key Features

One contact per email (emails normalized to lowercase)

Events merged across multiple platforms

Name conflicts resolved using the most recent event

Reply status and latest reply tracked

Timestamps normalized to ISO 8601 UTC

Output sorted by email, then first outreach time

Storage

Uses SQLite for simple, reliable local persistence and incremental updates.

Output

Exports a single unified.csv file with a fixed schema, sorted deterministically and ready for analysis.

Assumptions

Events may arrive out of order

Missing optional fields are stored as empty strings

Only the most recent reply information is retained