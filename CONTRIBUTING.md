# Contributing Guide

Thank you for contributing to **Voice Agent Control**!

## Adding a Business Domain Cartridge (YAML)

1. Create a new YAML file under `project/app/domains/<workflow_id>.yaml` (see `new_car_sales.yaml` and `car_loan.yaml` for reference).
2. Configure:
   - `domain_master_prompt`: core business objective & routing guidance
   - `enabled_sub_evaluators` & `custom_evaluators`: structured signals extracted concurrently after each call
   - `allowed_tools` & optional `declarative_http_tools`: strictly scoped PydanticAI tools
   - `allowed_handoff_targets` & `max_handoffs`: cross-workflow transfer rules
3. Validate the cartridge safely before production via `POST /workflow/{workflow_id}/dry-run`.

## Running Tests

```bash
PYTHONPATH=project .venv/bin/pytest project/tests/ -v
```
