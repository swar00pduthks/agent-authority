# ADR 0004: Sentinel rules are deterministic code, not models

**Status:** accepted

## Context
"Agent police" built from LLMs can be prompt-injected by the very agents they watch, and their verdicts are hard
to reproduce for an auditor.

## Decision
Sentinel rules are plain Python functions over captured lineage. Models may *suggest* rules or summarise findings
in future, but detection and any enforcement hook stay deterministic.

## Consequences
* Findings are reproducible and explainable: each one links to runs, a chain of authority and replayable events.
* Rules are less flexible than a model. That is the point.
