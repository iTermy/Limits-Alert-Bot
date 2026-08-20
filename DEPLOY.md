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

Not touched by the reset: `.env`, the venv and logs (gitignored), and the seven
pinned config files below (`reset --hard` respects their `skip-worktree` flags).

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
file. `.env`, the venv, and the logs are gitignored and stay put throughout.

Now protect the config files the bot rewrites at runtime, so pulls never clobber
your live tuning:

```
git update-index --skip-worktree config/settings.json config/tp_configuration.json config/alert_distances.json config/nm_configuration.json config/trailing_configuration.json data/news_events.json data/info_embeds.json
```

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

## Why those seven files are pinned

They are tracked in git but rewritten on the VPS at runtime:

| File | Written by |
|------|-----------|
| `config/settings.json` | `!goldtollssl`, `!riskygoldsl`, news commands |
| `config/tp_configuration.json` | `!tp set` / `!tp remove` |
| `config/alert_distances.json` | `!alertdist set` / `remove` |
| `config/nm_configuration.json` | `!nmconfig set` / `remove` |
| `config/trailing_configuration.json` | trailing config writes |
| `data/news_events.json` | `!news`, the 30 s cleanup loop |
| `data/info_embeds.json` | info-embed message IDs |

`BaseThresholdConfig` also rewrites its file on load when defaults are backfilled,
so these go dirty just from starting the bot. Without `skip-worktree` every pull
would abort on "local changes would be overwritten".

`config/channels.json`, `config/symbol_mappings.json` and `config/vol_guard.json`
are read-only at runtime and deploy normally.

### Deploying a change to a pinned file

Pulling a commit that touches a pinned file fails with "Entry ... would be
overwritten by merge". Unpin it, take the repo's version, re-pin:

```
git update-index --no-skip-worktree config/tp_configuration.json
git checkout -- config/tp_configuration.json
git pull --ff-only
git update-index --skip-worktree config/tp_configuration.json
```

To see what is currently pinned: `git ls-files -v | findstr /b S`
