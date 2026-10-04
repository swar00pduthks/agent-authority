# ADR 0001: OpenLineage is the only wire format

**Status:** accepted

## Context
Lineage emitters already exist for Spark, Airflow, dbt, Flink and more. A new hub only gets adopted if teams
can point existing emitters at it without code changes.

## Decision
Agent Authority ingests standard OpenLineage `RunEvent`s over HTTP (`POST /api/v1/lineage`, Marquez-compatible) and Kafka.
Agent context is carried in a run facet (`agent`) and hierarchy in the standard `ParentRunFacet`. There is no
proprietary event format.

## Consequences
* Any OpenLineage consumer can read events produced by the Agent Authority SDK.
* Optimisation happens behind the API (ADR 0002), never by asking emitters to change.
* The agent facet must track the community proposal; its name is configurable.
