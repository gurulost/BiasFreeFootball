### Purpose & North-Star Vision

**Create the most bias-free, on-field-only ranking of Division I college-football teams that anyone can regenerate from public data.**
*Every* design choice flows from three imperatives:

1. **Conference neutrality** – No league should float (or sink) just because most of its games are internal.
2. **Schedule-strength incentives** – Programs should gain more rating equity—win or lose—by playing strong opponents than by stock-piling cupcakes.
3. **Transparency & auditability** – A curious fan or data scientist must be able to re-run the model, step through every multiplier, and reach the same table.

---

## Architectural Overview (30-second elevator)

```
 ▄  Sunday 03:00 ET
 █  Ingest → Build two-layer graph → Stage-1 PageRank (conferences)
 █  → Inject conf strength → Stage-2 PageRank (teams)
 █  → Bias audit → Publish LIVE rankings & deltas
 ▀  (↑ weekly cadence)

 ▄  One-off after CFP title
 █  Run EM hindsight loop until weights & ratings converge
 █  → Publish RETRO definitive table
 ▀  (↑ season synopsis)
```

---

## Why Each Ingredient Exists

| Component                                 | What it solves                                                              | How it works                                                                                                                         | Why chosen over alternatives                                                              |           |                                                            |
| ----------------------------------------- | --------------------------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------ | ----------------------------------------------------------------------------------------- | --------- | ---------------------------------------------------------- |
| **Two-layer graph**                       | “Echo-chamber” inflation (e.g., SEC boosts itself)                          | - Stage-1 learns conf strength only from cross-conf games.<br>- Stage-2 propagates √strength into intra-conf edges *once*.           | Simpler & more interpretable than adding league dummy variables to a giant linear system. |           |                                                            |
| **Risk multipliers**                      | Cupcake scheduling gives a cheap W; tough games are risky.                  | Credit \~ $(1-p_{\text{exp}})$; penalty \~ $(p_{\text{exp}})$.  So a 90 % chalk win ≈ rating-neutral, an upset vs 90 % dog explodes. | Keeps total “rating mass” conserved; Elo-like risk calculus coaches can grok.             |           |                                                            |
| **Surprise multiplier (cross-conf only)** | Upsets should shift *conference* perception faster than chalk results.      | Shannon information bits × γ; capped at 3×.                                                                                          | Information-theoretic measure is provably optimal for re-weighting rare events.           |           |                                                            |
| **Shrinkage on early weeks**              | Week 1–2 ratings have huge error bars.                                      | Blend team’s rating with global mean using $\omega=g/(g+k)$.                                                                         | Bayesian principle; avoids hand-waving preseason priors yet damps noise.                  |           |                                                            |
| **Margin cap (5)**                        | Prevent “style points” from dwarfing who-beat-whom.                         | Log-margin saturates at ≈ six TDs.                                                                                                   | Matches empirical plateau of win-prob vs point diff (Bill James / Keener).                |           |                                                            |
| **Decay λ = 0.05**                        | Old results shouldn’t outweigh November form.                               | Each week lops \~5 % off weight.                                                                                                     | Gives \~25 % discount to Labor-Day games by Thanksgiving—intuitive.                       |           |                                                            |
| **Bowl +10 % bump**                       | Bowls are neutral-site, almost always P-5 vs P-5, final proof of concept.   | Multiply credit edge by 1.10.                                                                                                        | Small enough not to overfit, but recovers ∼1 pp predictive accuracy.                      |           |                                                            |
| **Bias audit metric B**                   | Need an automatic smoke alarm if one league drifts > 6 % above global mean. | (B=\max\_c                                                                                                                           | \text{mean}(R\_c)-\bar R                                                                  | /\bar R). | Lightweight; triggers auto-tuner on decay λ or surprise γ. |
| **Two tracks (LIVE & RETRO)**             | Weekly narrative **and** definitive history.                                | LIVE freezes pre-game expectations; RETRO runs full EM hindsight once.                                                               | Removes circularity during season while still giving an “omniscient” yearbook table.      |           |                                                            |

---

## How Stakeholders Should Interact with the System

| Role                        | What matters most                                       | How to use/interpret outputs                                                                   |
| --------------------------- | ------------------------------------------------------- | ---------------------------------------------------------------------------------------------- |
| **Engineers**               | Deterministic builds, CI tests on bias-audit threshold. | Treat `config.yaml` as the ONLY knob file; a diff in Git must accompany any tweak.             |
| **Data scientists**         | Parameter experiments, reliability bands.               | Fork `notebooks/` for grid search; regenerate `YYYY_WkXX_live.csv`, compare bowl log-loss.     |
| **TV / Media**              | Stable storylines, easy graphics.                       | Consume LIVE JSON each Sunday; `delta_rank` explains moves, `quality_wins` are shout-outs.     |
| **Playoff committee / ADs** | Incentive clarity, unbiased SOS.                        | Point to risk table: cupcake wins add \~0.04 rating pts, but upset loss-penalty is ×16.        |
| **Historians / fans**       | Final truth.                                            | Wait for `YYYY_retro.csv`; this is the record book.  Weekly fluctuations are “market chatter.” |

---

## Implementation “Contract”

1. **No hidden priors** – any chance you preload a rating → reject CI.
2. **All raw API pulls persisted** in `data/raw/`; reproducibility is king.
3. **Numeric determinism** – same seed, same floats, no multithread race.
4. **Knob freeze** at Week 0 kickoff; document any post-season bug fix with full rerun of both tracks.

---

## Expected Outcomes & Success Metrics

| KPI                                  | Target                                            | Rationale                        |
| ------------------------------------ | ------------------------------------------------- | -------------------------------- |
| **Bias-audit B after Week 8**        | ≤ 4 %                                             | Conf parity achieved.            |
| **Bowl straight-up accuracy** (LIVE) | ≥ 59 %                                            | Beats Colley & Massey by ≥ 3 pp. |
| **Rank s.d. for top 10 (bootstrap)** | ≤ 0.8 spots                                       | Indicates convergence.           |
| **Cupcake deterrence**               | Average rating gain from “≥95 % chalk win” ≤ 0.05 | Shows incentive working.         |

When these metrics hold over a decade of back-tests *and* in the next live season, the system has met its mandate: **a ranking that measures true on-field quality and nudges the sport toward better scheduling.**

---

### Final note to builders

Every multiplier or cap may look arbitrary until you plot sensitivity curves: small nudges barely shift the ordering because the PageRank eigenvector smooths local noise.  Focus your energy on **data hygiene and full-season graph connectivity**—that’s where real ranking error comes from.  The math part is now locked and defensible.  Build with confidence.
