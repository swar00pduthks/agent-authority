# Contributing

Thanks for helping. AgentHub aims to be the place where data lineage and agent accountability meet, so issues and
PRs from both the data-platform and the agent side are welcome.

## Dev setup

```bash
cd backend && pip install -e ".[dev,prod]" langchain-core
agenthub demo --no-browser          # API + demo traffic on :8000
cd ../ui && npm install && npm run dev   # UI with hot reload on :5173 (proxies /api to :8000)
```

## Tests

```bash
cd backend
ruff check agenthub tests
pytest -q                                                    # memory store, API, SDK, LangChain
AGENTHUB_TEST_PG_DSN=postgresql://... pytest tests/test_prod_store.py   # ClickHouse (embedded) + Postgres parity
cd ../ui && npx tsc -b && npm run build
```

Any change to storage must keep `tests/test_prod_store.py` green: the in-memory and production stores must give
identical answers.

## Adding a Sentinel rule

1. Add a function decorated with `@rule("id", "severity", "description")` in `backend/agenthub/sentinel.py`.
2. Yield `Finding`s with a `run_id` or `node_id` so the UI can link to evidence.
3. Add a scenario to `tests/test_governance.py`, and ideally to `agenthub/demo.py` so it shows up in the demo.

## Pull requests
- Keep PRs focused, and describe the user-visible change.
- Use [Conventional Commits](https://www.conventionalcommits.org/) (`feat:`, `fix:`, `docs:` ...).
- Discuss larger design changes in an issue first, and record them as an ADR in `docs/adr/`.

## Code of conduct
This project follows the [Contributor Covenant](https://www.contributor-covenant.org/version/2/1/code_of_conduct/).
See [CODE_OF_CONDUCT.md](CODE_OF_CONDUCT.md).
