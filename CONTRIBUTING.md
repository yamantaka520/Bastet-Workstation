# Contributing to Bastet Workstation

Contributions must follow [`docs/MASTER_PLAN.md`](docs/MASTER_PLAN.md). Work starts from the active milestone and may not claim later roadmap capabilities as delivered.

## Before changing the repository

1. Preserve unrelated local changes.
2. Record architecture changes as an ADR and update the Master Plan decision log when an accepted plan decision changes.
3. Keep third-party provenance and license information current.
4. Never commit secrets, credential material, private prompts, or unredacted logs.

## Validation

Run before requesting review:

```sh
python3 scripts/check_m0.py
python3 -m unittest discover -s tests -v
```

Delivery claims require evidence tied to the exact revision. Agent prose is not test evidence.
