# Railway SAFE-FAST automation pack

This pack automates the manual loop you were doing:

1. swap in a candidate `main.py`
2. push it to the Railway-connected branch
3. wait for Railway to finish deploying
4. hit `/safe-fast/on-demand` with the fixed payload
5. validate the JSON shape
6. rollback automatically if validation fails

## Files

- `.github/workflows/railway-safe-fast-validation.yml`
- `scripts/validate_safe_fast.py`

## Secrets to add in GitHub

Add these repository secrets:

- `SAFE_FAST_APP_URL`
  - Example: `https://safe-fast-backend-production.up.railway.app`
- `SAFE_FAST_HEALTH_PATH`
  - Usually `/health`
- `SAFE_FAST_ENDPOINT_PATH`
  - Usually `/safe-fast/on-demand`

## Repo layout expected

Recommended layout:

- `main.py` -> the file Railway deploys
- `candidates/` -> place candidate patch files here
- `.github/workflows/railway-safe-fast-validation.yml`
- `scripts/validate_safe_fast.py`

## How to use it

### Option A: Manual button in GitHub Actions
Run the workflow manually with:

- `candidate_file`: path to the candidate file you want tested
- `target_branch`: the Railway auto-deploy branch, for example `railway-test`
- `baseline_file`: your known-good fallback file

The workflow will:
- copy the candidate into `main.py`
- push it to the target branch
- wait for Railway to become healthy
- call the SAFE-FAST endpoint
- validate the response
- rollback if validation fails

### Option B: Push-driven
Use the same workflow with Railway auto-deploying from a test branch.
Then your process becomes:

- commit candidate file
- run workflow
- let the workflow stage, validate, and rollback if needed

## Rollback rule

Rollback is triggered when **either** of these happens:

- Railway never returns healthy on `/health`
- the validator fails any required response check

The rollback action restores the known-good `main.py`, commits it, and pushes it back to the Railway branch.

## Notes

- This is intentionally built around the exact manual loop you were already using.
- The validator is strict on the current no-candidate cleanup track because that is where most regressions were happening.
- You can relax or expand the validator later once the current backend branch is stable.
