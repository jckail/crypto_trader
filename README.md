# Crypto Trader

A historical Python learning project for cryptocurrency and market-data ingestion. The committed implementation contains an `alpha` ingestion runner and `omega` query/regression experiments. No frontend, trading order execution or production deployment is established by this documentation.

- [Architecture](docs/architecture.mdx): runner, provider, file and AWS boundaries.
- [Developer guide](docs/developer-guide.mdx): inert inspection and runtime prerequisites.

## Current implementation

| Area | Source-backed behavior |
| --- | --- |
| [Alpha runner](alpha/mt_alpha_runner.py) | Provider adapters, local setup, catalog/bucket preparation, optional crawler and log upload |
| [Persistence](alpha/savetos3.py) | Reads existing object/local data, converts tabular data to JSON and uploads to S3 |
| [Glue](alpha/gluemaintenance.py) | Checks/creates and starts crawlers through AWS clients |
| [Omega](omega/athena_test.py) | Athena query/result-download experiment with top-level calls |
| `alpha/inactives/` | Historical prototypes, including five existing Python syntax failures |

[runner.sh](runner.sh) activates a historical Anaconda environment and invokes [alpha_runner.sh](alpha/alpha_runner.sh). The selected wrapper enables ingestion and the crawler. These commands are operational; disabling the crawler does not disable provider calls, local writes, AWS setup or uploads.

## Inspect without running ingestion

With Python 3, from the repository root:

```bash
python3 scripts/check_docs.py
```

The dependency-free checker parses 25 top-level alpha scripts and three omega scripts, reports known inactive syntax gaps by filename/line, and checks documentation links/fences and inventory. It never imports application modules, reads datasets or prints source literals. Syntax acceptance does not qualify providers, AWS, analytics, trading, MDX rendering or deployment.

## Historical setup and scope

[requirements.txt](requirements.txt) is a large historical environment export, not a newly qualified minimal installation recipe. Source uses old directory/environment assumptions and literal credential configuration; review locally and replace with inert injected clients before behavior testing. The original crypto-learning/analytics intent is retained, while omega experiments and historical prototypes remain distinct from a verified strategy or executable trader. No dependency upgrade, source repair, live account setup or runtime operation is included here.
