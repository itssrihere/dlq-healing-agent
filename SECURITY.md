# Security policy

DLQ Healing Agent is a personal portfolio project. It has not been run on production traffic and makes no production-hardening
claims. Security reports are still welcome, and I will treat them seriously on a best-effort basis (no SLA).

## Supported versions

Only the `main` branch. There are no releases to patch.

## How to report a vulnerability

Please **do not open a public issue** for a security problem.

1. Open the **Security** tab of this repository and choose **Report a vulnerability** (GitHub's private vulnerability reporting).
2. If that button is not there, open a public issue that says only *"I would like to report a security problem privately"*, with
   no details, and I will arrange a private channel.

Please include what you found, how to reproduce it, and the impact you expect. **Never include real secrets, API keys or real
customer data in a report.** I aim to reply within about a week; this is a spare-time project, so I cannot promise more.

## What is in scope

Reports about any of these are exactly what I am looking for:

- bypassing the evaluator gate so that a wrong or unvetted payload is replayed automatically;
- replaying to a topic other than the stream's own source topic (for example through the `original-topic` header);
- authentication or authorisation problems in the approval and audit APIs (`/api/approvals/**`, `/api/audit/**`);
- altering or deleting audit rows, or approving a held event twice;
- injection through event content (log injection, SQL, JSON handling) and unbounded resource use from hostile payloads or headers;
- anything that exposes a secret, or a vulnerable dependency that the daily software-composition scan misses.

## Known and documented, so not reportable on their own

- **The default local database credentials** (`dlqagent` / `dlqagent`) in `docker-compose.yml` and `application.yml` are for local
  development and tests only. The approval API's password has no default and the application refuses to start without one.
- **One reviewer account.** The approval API has a single Basic-auth account; per-stream reviewer roles are listed as future work.
- **Prompt injection can steer the model's classification.** The evaluation observed it (see "Labelled evaluation" in the README).
  The design assumes model output is untrusted: a steered answer can pick a different strategy, but the deterministic gate and the
  retry budget still decide what is replayed. A *bypass of the gate* is in scope; a model being talked into a different
  classification is a known limitation.
- **At-least-once delivery.** A replay can be sent twice (for example after a crash before the offset commit); the downstream consumer
  must be idempotent. This is documented behaviour.
- **The demo and evaluation scripts** use throwaway containers and throwaway credentials, and read `OPENAI_API_KEY` only from your environment.

## Security measures already in place

SpotBugs with the FindSecBugs rules gates every build; a CycloneDX SBOM is scanned daily by Syft and Grype in CI; Dependabot proposes
dependency updates; the GitHub Actions are pinned to commit SHAs. Details are in the README under "Static analysis and hardening" and
"Supply chain".
