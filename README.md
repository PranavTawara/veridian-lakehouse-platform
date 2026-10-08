# Veridian Finance DE Platform — <your name>

Replace this whole file with your own. Two things must survive whatever you write:

1. **How to run everything, from a clean checkout.** Assume the reader has Docker and nothing else
   pre-installed. If a step needs a password/port/env var, say what and where.
2. **A short summary of the decisions you'd defend**, the trade-offs you accepted, and what you'd do
   differently with more time. A page is plenty — see the assignment brief for the exact ask.

Everything else in this workspace (`docs/`, `logs/`, and the per-layer folders below) is a suggested
starting shape, not a checklist you must fill in completely. Reorganize, rename, delete, or ignore any
of it — the brief says so, and it means it. The one part that isn't optional is keeping
`logs/engineering_log.md` current as you actually work, not reconstructed at the end.

## Suggested layout (yours to change)

```
docs/
├── data_model.md         your schema + reasoning
└── decisions/            one note per major decision — see docs/decisions/0001-example.md
logs/
└── engineering_log.md    dated, running log — start writing here on day one
ingestion/
transformation/           e.g. a dbt project or equivalent
orchestration/            DAGs / flows / scheduler config
api/
dq_checks/
recon/
docker-compose.yml        or equivalent, for local run
```
