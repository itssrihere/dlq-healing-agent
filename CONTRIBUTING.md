# Contributing

This is a personal portfolio project, so there is no roadmap commitment and I cannot promise to review quickly. Issues, questions and
pull requests are welcome all the same. For a security problem, see [`SECURITY.md`](SECURITY.md) instead of opening an issue.

## Build and test

You need JDK 21 and Docker (the integration tests start their own Kafka and PostgreSQL with Testcontainers). Use the included wrapper:

```bash
export JAVA_HOME=$(/usr/libexec/java_home -v 21)   # macOS; any JDK 21
./mvnw test          # unit tests only: fast, no Docker
./mvnw verify        # unit + integration tests + the SpotBugs / FindSecBugs gate; this is what CI runs
./scripts/demo.sh    # a guided run against a real model (needs OPENAI_API_KEY; about ten model calls)
```

Tests never call a real model. The live-model tests and the labelled evaluation are opt-in (`DLQ_LIVE_LLM=true`, `DLQ_EVAL=true`) and
cost a little money; see the README and `src/test/resources/eval/README.md`. **Never commit an API key.** GitHub's push protection
catches many keys but is not a guarantee, and a key that was ever committed has to be rotated.

## Before you open a pull request

- `./mvnw verify` passes, including SpotBugs. An exclusion in `spotbugs-exclude.xml` needs a written reason.
- New behaviour has a test, and where a test guards a safety property, check that it can fail: break the thing it guards and see the
  test go red. Several tests in this repository were found weak that way.
- If behaviour changes, update the README. Claims in this project are only made for things that were actually run, with their limits
  stated; please keep to that.
- **Do not edit a published evaluation dataset** (`cases.jsonl`, `cases-v2.jsonl`): `EvalFreezeTest` will fail. Copy to a new version
  instead; the procedure is in `src/test/resources/eval/README.md`.
- The evaluation tables in the README are generated: after changing a report in `docs/eval/`, run
  `python3 scripts/eval_tables.py --write` (CI runs `--check`).
- Match the style of the surrounding code, and keep commits focused with a message that says why.
- If a change was written with AI assistance, say so in the pull request, as this project does about itself.

## Dependencies

Dependabot opens the update pull requests. Major upgrades of Spring Boot, Spring AI, `json-schema-validator` (3.x needs Jackson 3) and
the PostgreSQL image (Flyway has not tested 18) are held on purpose; the reasons are in `.github/dependabot.yml`.

## Licence

By contributing you agree that your contribution is licensed under the [MIT licence](LICENSE).
