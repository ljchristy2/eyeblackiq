# EyeBlackIQ — CHANGELOG

---

## [v0.8.0] — 2026-03-21
**Task ID:** CC-UI-20260321-005 / CC-PIPELINE-20260321-001
**Triggered by:** Master chat — Sprint: BYU DB fix, export improvements, enrich_results ETL, UI streak/grouping upgrades
**Summary:** (1) Fixed signal id 100 (BYU @ WVU) — corrected from ML +1400 to SPREAD BYU +1.5 (mispriced ML). (2) export.py: added `bet_type`/`pick_source` to pod_summary; added game_time to ORDER BY for chronological sorting; added comprehensive streak computation to record (pod_streak, day_streak, sport_streaks, type_streaks); added variance_verdict/variance_note to LOSS records. (3) New `pipeline/enrich_results.py` — ESPN box score fetcher for NHL/MLB/NCAA_BASEBALL/Soccer; fuzzy team matching; writes actual_val to results table. (4) index.html: SPORT_ICONS map + sportIcon() helper; DAY STREAK and POD STREAK added to record bar; renderPicks() upgraded — tab-specific POD filtering, chronological sort, sport-grouped picks view, contextual empty states. (5) results.html: fixed isProp() bug (was using pick_source, now uses bet_type); upgraded buildLossAnalysis() with structured HTML — box score FINAL, miss margin, variance verdict badge, CLV line, context notes; per-sport streaks in renderPodBadges(); loadRecord() captures sport_streaks/type_streaks/pod_streak/day_streak; DAY STREAK + POD STREAK added to record bar; .la-structured/.la-score CSS added. (6) style.css: .sport-pick-group, .sport-pick-header, .sport-pick-count CSS added.

**Files Modified:**
- `pipeline/db/eyeblackiq.db` — signal id 100 updated (bet_type=SPREAD, side=BYU +1.5, odds=110)
- `pipeline/export.py` — pod_summary adds bet_type/pick_source; ORDER BY adds game_time; export_record() adds pod_streak/day_streak/sport_streaks/type_streaks; export_results() adds variance_verdict/variance_note
- `pipeline/enrich_results.py` — NEW FILE: ESPN box score enrichment ETL for results.actual_val
- `docs/index.html` — SPORT_ICONS constant + sportIcon(); DAY STREAK + POD STREAK in record bar; updateRecordBar() updated; renderPicks() full rewrite (tab-POD filter, chrono sort, sport groups, contextual empties)
- `docs/results.html` — isProp() bug fix; buildLossAnalysis() structured HTML rewrite; renderPodBadges() adds streaks + globals; loadRecord() captures new streak fields; record bar adds DAY/POD STREAK; .la-structured/.la-score CSS
- `docs/style.css` — .sport-pick-group, .sport-pick-header, .sport-pick-count
- `docs/data/record.json` — regenerated with sport_streaks, type_streaks, pod_streak, day_streak
- `docs/data/today_slip.json` — regenerated with pod[].bet_type and pod[].pick_source

**Schema Changes:** No
**Backtest Impact:** Not required
**Go-Live Parameter Changes:** No
**Tests Passed:** Yes — export: 67 recommended, 1 flagged, 1 POD; record.json has sport_streaks/type_streaks/pod_streak/day_streak; signal 100 = SPREAD BYU +1.5 @110; enrich_results.py runs cleanly (0 missing rows in current window)
**Pod Version:** UI v2.4.0 | Pipeline v0.8.0

---

## [v0.7.0] — 2026-03-21
**Task ID:** CC-NHL-20260321-002 / CC-UI-20260321-004
**Triggered by:** Master chat — Props fix sprint (classification, SOG model upgrade, UI polish)
**Summary:** (1) Fixed isProp() classification bug — Props tab now shows ONLY bet_type ending in `_PROP`; team ML/totals/spreads can no longer bleed into Props tab. (2) Fixed edge_window() prop cap bug — `_PROP` suffix now correctly triggers 30% prop cap (was 20% team cap); 2 SNIPEs >20% edge un-stuck from flagged_high. (3) Goals/Points/Anytime/FirstGoal props removed from DB write path — no model, no edge, no noise; 318 0-edge stale records purged. (4) Player-specific Poisson lambda — NHL API skater stats endpoint (shots/gamesPlayed); positional fallback F=2.6, D=1.9; 11 player-stat matches, 32 league-avg fallback in today's run. (5) Props sub-filter row [SOG][Goals][Points][All Props] added — defaults to SOG when Props tab clicked. (6) propCard() renderer — player name, team abbreviation, sportsbook source badge (FD/DK), SOG avg/game, game-grouped display within Props tab. (7) Pick'em empty state updated to "Coming Soon". (8) DraftKings confirmed SPA-blocked (Cloudflare 403); TheRundown covers DK odds April 1.

**Files Modified:**
- `scrapers/scrape_fanduel_props.py` — FULL REWRITE v0.7.0: SOG-only; player-specific λ from NHL API; Goals/Points/Anytime removed; propCard-compatible notes format; DK documented as SPA-blocked
- `pipeline/export.py` — Fixed edge_window() cap: `_PROP` in bet_type → 30% prop cap; added edge==0 AND not is_pod guard; fixed flagged_high cap_pct display
- `docs/index.html` — Fixed isProp() to check bet_type.endsWith('_PROP'); added isTeamLine(); added prop_sub filter state; added prop-sub-bar HTML; added propCard() with player/team/source/SOG-avg; updated matchesTypeFilter() for prop_sub; game-grouped props render; Pick'em "Coming Soon" state; props empty state with sub-label
- `docs/style.css` — Prop card styles: .prop-player-row, .prop-player-name, .prop-team-tag, .prop-threshold, .prop-sog-avg, .prop-src-badge (FD/DK), .prop-game-group, .prop-game-header, .prop-game-picks; prop-sub-bar style

**Schema Changes:** No (DB cleanup only — 318 0-edge Goals/Points rows deleted)
**Backtest Impact:** Not required
**Go-Live Parameter Changes:** No
**Tests Passed:** Yes — 43 SOG props (16 SNIPE, 17 SLOT MACHINE, 10 SCOUT); 2 >20%-edge SNIPEs now in recommended[]; 0 Goals/Points in recommended[]; all checks passed; export: 67 recommended, 1 flagged, 1 POD; 69,700 bytes
**Pod Version:** NHL props v0.2.0 | UI v2.3.0

---

## [v0.6.0] — 2026-03-21
**Task ID:** CC-NHL-20260321-001 / CC-UI-20260321-002 / CC-UI-20260321-003
**Triggered by:** Master chat — FanDuel props scraper + UI overhaul + Validation tab
**Summary:** (1) FanDuel public API scraper for NHL player props — 369 signals (SOG 2+/3+/4+/5+, Goals, Points, Anytime Scorer) written to DB for Mar 21; (2) UI overhaul — Upcoming Picks section separated from Today's Picks, On Radar/FMV sections elevated with colored borders and auto-expand; (3) Validation tab created with backtesting results for all sports; (4) Playbook updated — Cricket/Handball sections added, Boyd's World (ISR) and Warren Nolan (ELO) cited as sources; (5) Nav updated across all 4 pages.

**Files Modified:**
- `scrapers/scrape_fanduel_props.py` — NEW FILE: FanDuel NHL public API scraper; parses SOG/Goals/Points/Anytime markets; writes to DB using correct signals schema; 369 props parsed Mar 21
- `docs/index.html` — Validation nav link; Upcoming Picks section (future signal_date); On Radar redesign (blue border, auto-expand); Full Market View redesign (gold border); sport icon map confirmed; footer updated
- `docs/results.html` — Validation nav link added; footer updated
- `docs/methodology.html` — Cricket + Handball sections added; Free Odds Pipeline section; Boyd's World + Warren Nolan citations; Pick'em Markets updated to source-based; Current Status updated
- `docs/validation.html` — NEW FILE: Backtest results for Handball (Brier 0.1936), Cricket International (Brier 0.2330), NHL Team ML (paper trading), NCAA Baseball (paper trading); walk-forward methodology; data sources
- `docs/style.css` — On Radar/FMV sections redesigned (bordered, colored); Upcoming Picks section added; radar/FMV count badges colored

**Schema Changes:** No
**Backtest Impact:** Not required
**Go-Live Parameter Changes:** No
**Tests Passed:** Yes — FanDuel scraper: 369 signals written; export: 75 recommended, 319 flagged; all pages load clean
**Pod Version:** NHL props v0.1.0 | UI v2.2.0

---

## [v0.5.1] — 2026-03-21
**Task ID:** CC-CRICKET-20260321-002 / CC-HANDBALL-20260321-002 / CC-HANDBALL-20260321-003
**Triggered by:** Master chat — Cricket ELO fix + Handball upcoming signals + Calibration
**Summary:** Three improvements: (1) Cricket ELO separated into FRANCHISE vs NATIONAL pools; (2) Handball upcoming fixtures fetcher with forward-looking signals written to DB; (3) Platt scaling calibration added to handball model with before/after backtest.

**Files Modified:**
- `scrapers/fetch_historical_cricket.py` — FRANCHISE_TEAMS set, compute_separated_elos(), run_backtest_separated(), team_type column written on every upsert
- `scrapers/fetch_handball_upcoming.py` — NEW FILE: API Sports + ESPN + hardcoded EHF CL QF fallback; generate_forward_signals()
- `pods/handball/model.py` — platt_calibrate(), run_calibration_backtest(), PLATT_SHRINK/PLATT_ENABLED constants, sys import, --calibrate CLI flag; MODEL_VERSION bumped to 1.1.0

**Schema Changes:** Yes — `cricket_team_stats.team_type TEXT DEFAULT 'NATIONAL'` column added (ALTER TABLE + new schema definition)

**Backtest Results:**
- Cricket IPL Franchise ELO: Brier=0.25519 (n=1,117 franchise matches, 18 teams)
- Cricket International ELO: Brier=0.23290 (n=3,111 national matches, 107 countries)
- Handball Calibration: Brier_raw=0.18022; Platt shrink=0.82 worsens score (-3.59 millibrier) — PLATT_ENABLED=False pending more data; overconfidence found at LOW probabilities (0-30% range), underconfidence at HIGH end (90%+)

**Handball Forward Signals:** 4 written to signals table (EHF CL QF approx dates 2026-04-02/09); pick_source=MODEL_FORWARD; no market odds — edge vs 50/50 baseline

**Backtest Impact:** Not required (improvements to existing computation, not breaking changes)
**Go-Live Parameter Changes:** No
**Tests Passed:** Yes (--elo-only, --backtest, --calibrate all ran clean)
**Pod Version:** Cricket scraper v0.5.1 | Handball model v1.1.0

---

## [v0.5.0] — 2026-03-21
**Task ID:** CC-CRICKET-20260321-001 / CC-HANDBALL-20260321-001 / CC-UI-20260321-001
**Triggered by:** Pod chat — Cricket & Handball Efficiency Notes PDF + UI bug fixes
**Summary:** Added full Cricket and Handball prediction pods (models + historical scrapers + DB schema); fixed results.html date contamination, Props/Pickems classification logic, sport filter buttons, and POD badge rendering.

**Files Modified:**
- `pods/cricket/model.py` — NEW: Resource-Value model (Par Score, Venue Z-Factor, ZIP wickets, survival probability, T20 middle-order compression, ELO + Par blend 55/45)
- `pods/handball/model.py` — NEW: Efficiency-Flow model (possession SOS, adj xG, Poisson goals, ELO + Poisson blend 55/45, usage redistribution for injuries)
- `scrapers/fetch_historical_cricket.py` — NEW: Cricsheet T20/IPL parser + API Sports fallback; 4,390 T20 matches loaded; 126 team ELOs computed; data phase cleared
- `scrapers/fetch_historical_handball.py` — NEW: API Sports EHF CL + HBL + Starligue; 973 matches; 48 team ELOs; 152 team-season stat rows; data phase cleared
- `pipeline/db_init.py` — Added 8 tables: handball_matches, handball_team_stats, handball_odds, cricket_matches, cricket_innings, cricket_team_stats, cricket_venue_stats, cricket_players (15 tables total)
- `pipeline/db_migrate.py` — NEW: Non-destructive ALTER TABLE migration for existing DBs
- `pipeline/export.py` — Edge window active (ML/Totals 3–20%, Props 3–30%, PODs bypass)
- `pipeline/grade.py` — Minor grading fixes
- `approve_pod.py` — POD approval workflow updates
- `docs/index.html` — +Cricket 🏏 +Handball 🤾 sport filters; sport icon map expanded; Props UX: T3/Scout included in main grid when Props/Pickem type filter active
- `docs/results.html` — Fix isPickem/isProp to use pick_source field; date-group headers in Daily Results (newest first, clear separator); PENDING picks dimmed 50%; renderPodBadges +CRICKET +HANDBALL; +Cricket/Handball to all sport filter bars including FMV
- `docs/style.css` — Minor style tweaks

**Schema Changes:** YES — 8 new tables added (handball + cricket). Use `pipeline/db_migrate.py` for existing DB or re-run `pipeline/db_init.py` on fresh DB.

**Backtest Impact:** N/A — new pods in data phase; no existing backtests affected

**Go-Live Parameter Changes:** No

**Tests Passed:** Yes (manual verification — 4,390 cricket matches, 973 handball matches, ELOs computed, models return DATA_PHASE=cleared)

**Pod Version:** cricket v0.1.0 | handball v0.1.0

**Master Doc Update:** Not required (pod additions within scope of existing architecture)

---

## [v0.4.0] — 2026-03-20
**Task ID:** CC-UI-20260320-001
**Summary:** Edge window activated (3–20% ML/Totals, 3–30% Props, PODs bypass), Full Market View tab added to results.html, export.py FMV output, API Sports registry.

---

## [v0.3.0] — 2026-03-19
**Task ID:** CC-UI-20260319-001
**Summary:** Pick'ems tab + Props tab source-based routing, visual flags (B2B OPP, B2B PLR, EDGE HIGH, LOW EV), POD auto-rebuild, approve_pod.py workflow.

---
