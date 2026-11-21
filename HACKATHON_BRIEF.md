# Builders Buildathon: Shopping Sessions brief

Sources: https://www.joinbuilders.org/buildathon and https://www.joinbuilders.org/buildathon/tracks/shopping-sessions (read 21 Nov 2026). Leaderboard numbers are a snapshot.

## TL;DR
- This is **not** a "build a cool shopping app and pitch it" hackathon. It is a **data-engineering speed race** sponsored by Databricks.
- You get dirty e-commerce event logs (CSV). You must clean them, stitch together each shopper's multiple user IDs, build sessions, attribute sessions to ad clicks, count assisting campaigns, and write two Parquet tables.
- You submit a **Dockerfile**. A robot runs it on hidden data. **Score = run time in seconds, lower wins.** Output must be exactly correct, or the run scores nothing.
- **Winner takes $5,000. Second place gets nothing.** There are no human judges for this track.
- **Submissions close Sun 22 Nov 2026, 11:00 AM ET.**

## The event
- Run by Builders (joinbuilders.org). Pitch: "Most hackathons are LARP." They use real company-style problems with a live, auto-scored leaderboard instead of a judging panel.
- 20 to 22 Nov 2026 at Ohio State. Remote participation is allowed for the whole event.
- $20,000 total prize pool across 4 tracks: Mech Royale (Builders), **Shopping Sessions (Databricks)**, CAD De-ID (Machina), Blast Radius (RWX).
- Questions: hi@joinbuilders.org

## The task, step by step
Input: CSV logs in `/data` covering page views, add-to-carts, purchases and ad clicks, plus a third file that links user IDs together.

The data is deliberately messy: duplicates, malformed lines, bad timestamps, late-arriving events. One person can have many user IDs, linked through long chains and shared devices.

1. **Clean** the logs.
2. **Resolve identity**: join each person's IDs into one person. This is effectively connected components over the ID-link graph, so watch the long chains and shared-device edge cases.
3. **Sessionize** each person's events.
4. **Attribute** each session to the ad click that brought the shopper in.
5. **Count assisting campaigns** from the previous week.
6. **Write two daily Parquet tables**:
   - `/output/user_daily/`: one row per person per day
   - `/output/campaign_daily/`: one row per campaign per day

The exact definitions live in **`rules.md`**, which you download from the track page. There is exactly one right answer, and "fast but nearly right" scores zero. Read `rules.md` line by line before writing any code.

## Starters and checks
- Starter kits exist in **PySpark, DuckDB and Polars**. Each writes both tables with the right columns and zero rows.
- The starter comes with **36 edge-case fixtures**. Your solution must pass all of them.
- Any language or engine is allowed, with as much AI help as you like.

## How you are scored
- Your Dockerfile gets built, checked for correctness on the dev set, then timed once in a fresh container on hidden practice data.
- Score = container wall time from start to exit, in seconds. If a run is under 20 s, it is timed 3 times and the median counts. Ties go to the earlier submission.
- **Practice:** about 20M events, limited to 2 submissions per hour. Rejected submissions still count toward that limit. Only one entry per team can be in the queue at a time.
- **Final:** about 60M events, with a 3-minute cap. Going over the cap means no final score. The organizers suggest staying under about 60 s in practice.
- Baseline (PySpark starter, local mode): 172 s.
- **The leaderboard is already brutal:** the top practice scores are around **0.6 to 1.1 s**, across about 174 teams. To win, you need to be correct and near the top of that range on the 60M final.
- Pick your final entry before the cutoff. If you don't, your best-scoring entry is used. Only the final decides prizes, and organizers verify the top results.

## Key dates (Eastern Time)
| When | What |
|---|---|
| Fri 20 Nov, 6:00 PM ET | Kickoff (Evans Lab 1008). All tracks open. |
| **Sun 22 Nov, 11:00 AM ET** | **Submissions close.** Final entries lock. Video deadline too. |
| Sun 22 Nov, 12:00 PM ET | Closing ceremony (Hitchcock Hall 131). Winners announced and paid. |

## Rules that matter
- Only current students are eligible, and you register with your school email. Students in any country can enter.
- Teams of 1 or 2. Each person can be on only one team. A team can enter and win multiple tracks.
- Everything you build must be released under the **MIT License**. Pre-existing and open-source code is allowed. If the track takes a repo, credit that code in the README.
- Side prize: **$100 for best video**. Upload it to YouTube as Unlisted and enter it from the team page by 11 AM ET Sunday.
- Winners are paid online whether or not they attend.

## What this means for our strategy
1. **Correctness first.** Get all 36 fixtures passing with the simplest engine. DuckDB is probably the quickest path to correct SQL.
2. **Then go fast.** Sub-second times on 20M events suggest that top teams use a compiled, single-pass approach (for example Rust or C++, or highly tuned DuckDB/Polars), custom CSV parsing, multithreading, and a slim Docker image with minimal startup cost.
3. **Spend practice submissions carefully.** We only get 2 per hour, so validate locally against the fixtures and a large synthetic dataset before submitting.
4. **Watch the 60M final.** Make sure memory use and run time scale linearly, so we don't blow the 3-minute cap.
5. Optional: record a short video for the $100 prize.

## Links
- Track: https://www.joinbuilders.org/buildathon/tracks/shopping-sessions
- Main page and rules: https://www.joinbuilders.org/buildathon
- Leaderboard: https://www.joinbuilders.org/buildathon/leaderboard
- Status: https://www.joinbuilders.org/buildathon/status
- Team page: https://www.joinbuilders.org/buildathon/team
