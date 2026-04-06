SAFE-FAST ON-DEMAND PATCH

Patch purpose:
Fix the current runtime error:
NameError: name 'indicator_context' is not defined

What to edit:
GitHub → branch main → root file main.py

Exact edit:
In function `_build_on_demand_payload`, find the first place where the code passes:

    indicator_context=indicator_context,

Immediately ABOVE that section, insert this exact block with the same surrounding indentation level:

        try:
            indicator_context
        except NameError:
            indicator_context = {}

Why this works:
- It prevents the payload builder from crashing when `indicator_context` has not been assigned yet.
- It is the safest non-breaking guard from the current traceback.
- It lets the rest of the new candidate-context path continue rendering.

After commit, run this forced prompt:

Call getSafeFastOnDemand with:
{
  "option_type": "C",
  "open_positions": 0,
  "weekly_trade_count": 0
}
Return only the action result.

Expected proof:
- build_tag should remain:
  au_patch_candidate_context_structure_filters_2026_04_05
- candidate_context should include:
  - structure
  - filters
