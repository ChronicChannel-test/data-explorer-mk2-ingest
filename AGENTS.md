## Archive
- Files in `archive/` can be referenced for context but must never be modified once created. Adding new files/directories under `archive/` is allowed.
- Do not create archive copies for routine/small edits by default.
- Create archive snapshots before major/high-risk changes (for example: broad refactors, cross-repo updates, schema-shape migrations, or large workflow rewires), and whenever the user explicitly asks.
- For `/Users/mikehinford/Dropbox/Projects/CIC Website/CIC Data Explorer/CIC Data Explorer Mark 2/CIC-test-data-explorer-mk2-ingest`, edits are allowed for any file except under `archive/` directories. Archive files are read-only; new files may be added under `archive/` but must never be modified once created.
- The agent has permission to read files under `/Users/mikehinford/Dropbox/Apps/github-data-explorer-mk2` (including subdirectories).

## Planning Requests
- When proposing plans, offer more than one option when possible, list pros/cons for each, and recommend which to pick with a brief rationale.
- For every plan, explicitly assess both egress impact and database-size impact. Include those impacts in each option's pros/cons, and use them directly in the recommendation so tradeoffs are clear before implementation.

## Database Constraints
This project uses a Supabase DB with a 500MB limit.
Planning recommendations must account for this limit.
