"""Agent Authority - lineage and accountability for data pipelines and AI agents, built on OpenLineage."""

__version__ = "0.1.0"

from agent_authority.sdk import Authority, Dataset  # noqa: E402  (stdlib-only, safe to import eagerly)

__all__ = ["Authority", "Dataset", "__version__"]
