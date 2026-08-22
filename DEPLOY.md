# Deploying to the VPS

The VPS runs from a git clone of `iTermy/Limits-Alert-Bot`, branch
`stage20_professionalize`. Day-to-day deploys are: commit and push here, then run
`update.bat` there. No file copying.

---

## Day-to-day

**On the dev machine**

```
git push
```

**On the VPS**

1. Stop the bot (Ctrl+C in its window).
2. Double-click `update.bat` (or run it from the terminal).

It fetches, **hard-resets to `origin/stage20_professionalize`**, prints the commits
it picked up, reinstalls dependencies if `requirements.txt` changed, and starts
`main.py`. GitHub is the source of truth: local changes to tracked files are
discarded, so the deploy can never be blocked by a dirty tree or a missing upstream.

That includes the config files — see [Config is deployed, not tuned in
place](#config-is-deployed-not-tuned-in-place). Not touched by the reset: `.env`,
the venv, the logs, and `data/` (all gitignored).

Two details that make it safe to run at any time:

- **It re-execs itself from `%TEMP%` first.** The reset can rewrite `update.bat`
  while it is running, and cmd.exe reads batch files by byte offset — a file that
  changes underneath it corrupts execution from that point on. Running from a copy
  sidesteps that. The freshly pulled version takes effect on the next run.
- **`.gitattributes` forces `*.bat` to CRLF on checkout.** A batch file with LF-only
  line endings mis-parses in ways that are hard to spot (silently empty variables,
  `if` blocks running when they shouldn't), and the VPS's `core.autocrlf` setting
  can't be relied on.

---

## One-time setup: turn the copied VPS folder into a clone

Run these in the VPS bot folder, in order. Nothing is destructive until the last
step, and you get to review the diff before it.

**Back the folder up first** (a plain copy elsewhere) — that is the undo button.

```
git init
git remote add origin https://github.com/iTermy/Limits-Alert-Bot.git
git fetch origin stage20_professionalize
git checkout -b stage20_professionalize
git reset origin/stage20_professionalize
git branch --set-upstream-to=origin/stage20_professionalize
```

At this point git knows what the code *should* be but has not touched a single
file. `.env`, the venv, the logs and `data/` are gitignored and stay put throughout.

Review what is about to change:

```
git status --short
```

Everything listed is a difference between the VPS's files and the repo. It should
be stale code only. **If you see a VPS-only change you want to keep, copy it aside
now** — the next command overwrites it.

```
git checkout -- .
```

The VPS now matches the repo. Start the bot with `update.bat` and you are done.

---

## Config is deployed, not tuned in place

Every file in `config/` is overwritten from the repo on each deploy. The two
copies are meant to be identical, so the repo simply wins.

The consequence: **a threshold changed on the VPS through Discord survives only
until the next deploy.** `!tp set`, `!alertdist set`, `!nmconfig set`,
`!goldtollssl` and `!riskygoldsl` all write to `config/`, and `reset --hard`
discards those edits along with any other local change. Use them to try a value
live; commit the same change here to keep it.

`BaseThresholdConfig` also rewrites its file on load when defaults are
backfilled, so `config/` goes dirty just from starting the bot. `reset --hard`
does not care — it overwrites tracked files unconditionally and never aborts on a
dirty tree. (`git pull` would abort, which is why the day-to-day flow resets.)

### `data/` is not config

`data/news_events.json` and `data/info_embeds.json` are runtime state and are
gitignored — the VPS's copies are the only correct ones and no deploy touches
them. `info_embeds.json` holds live Discord message IDs; overwrite it with a
stale copy and the bot fails to find those messages and posts a second set of
info embeds beside the ones already in the channels. `news_events.json` holds
manual `!news` windows still running (auto-fetched ones are re-fetched at every
startup, never persisted).
