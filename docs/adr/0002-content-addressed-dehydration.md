# ADR 0002: Content-addressed dehydration of static facets

**Status:** accepted

## Context
OpenLineage events repeat large, rarely changing facets (schemas, SQL, Spark logical plans, producer URLs) on
every lifecycle event. In practice most of the stored bytes are repeats.

## Decision
Split facets into static and dynamic. Canonicalise static facets, hash them with SHA-256 (first 128 bits), store
each body once in `facet_blob`, and replace it in the event with `{"$ref": h}`. Intern the `_producer`/`_schemaURL`
envelope as `{"$meta": h}`. Keep a dehydrated copy of every kept event so the original can be rebuilt exactly.

## Consequences
* Storage scales with distinct content, not event count. ~12× on the bundled Spark workload.
* Replay needs a blob lookup per ref; acceptable for audit paths.
* 128-bit hashes make collisions negligible (birthday bound ~2^64 distinct facets).
* The static/dynamic classification is policy. Wrongly marking a dynamic facet static only costs dedup, never
  correctness, because each distinct body gets its own hash.
