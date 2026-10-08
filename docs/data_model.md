# Data model

Delete this scaffold once you've written the real thing — it's here to prompt what's worth covering,
not to be filled in section by section like a form.

## Source layer

How you're landing the raw CSVs (Bronze) — one table per source file, or something else? What did you
decide NOT to change at this layer, and why?

## Conformed / integrated layer (Silver)

This is the part the brief is explicit about: Silver is not ten cleaned copies of ten sources. What
are your conformed entities, and which source tables feed each one? For every join, be ready to
answer:

- What's the join key, and did it need cleaning first?
- What did you do with rows that matched more than once (fan-out)?
- What did you do with rows that matched nothing (orphans)?
- What did you do when two records for the same key disagreed (duplicate-key conflicts)?

## Consumption layer (Gold)

Facts and dimensions, wide reporting tables, something else — your call. For each table, what
business question does it answer, and how would someone else know that just from the model?

## Keys, types, and grain

Anything a downstream consumer would need to know to use these tables safely: grain of each table,
which columns are nullable and why, anything non-obvious about a key that isn't a straightforward
primary key.
