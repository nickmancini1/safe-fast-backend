# SAFE-FAST Replay / Regression Foundation

Purpose: freeze PATCH8 behavior and test future changes against saved replay cases before promotion.

This is build work only. It is not live trade evaluation. It does not patch the engine.

## Current baseline

- Frozen file from handoff: `main_preserve_locked_trigger_patch8_full.py`
- Repo live baseline: `main.py`
- Build tag: `macro_surface_v26_2026_04_21_preserve_locked_trigger_patch8`

## What replay checks first

The first regression layer checks stable decision labels only:

- ticker / winner
- verdict
- setup type
- recognition state
- tradeable true/false
- primary blocker
- trigger present true/false
- structure ready true/false

It intentionally ignores noisy fields like timestamps, option quote details, exact prices, long wording, and raw response text.

## Case status labels

- `captured`: real saved on-demand output is present and can be compared.
- `capture_needed`: expected label is defined, but real on-demand output still needs to be captured.
- `disabled`: intentionally not run.

## How to run

From the repo root:

```bash
python replay/replay_runner.py
```

Run one case:

```bash
python replay/replay_runner.py --case continuation_developing_hold_not_proven
```

## Promotion rule

Do not promote a future engine patch unless replay/regression passes or the expected-output file is deliberately updated with a documented reason.
