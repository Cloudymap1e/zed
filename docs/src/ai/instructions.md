---
title: Agent Instructions - Zed
description: Configure always-on personal and project instructions with AGENTS.md and compatible project instruction files.
---

# Instructions

Instructions are always-on context for agent interactions that read Zed-managed instruction files. Use instructions for persistent guidance that should apply to every relevant agent interaction.

Use [Skills](./skills.md) instead when you want reusable task instructions that can be invoked by name.

Zed supports [`AGENTS.md`](https://agents.md/) as the primary instruction file for personal and project-level agent guidance.

## Personal Instructions {#personal-instructions}

Personal instructions apply to projects opened in Zed when the selected agent integration reads Zed-managed instructions.

Create or edit:

```text
~/.config/zed/AGENTS.md
```

On Windows, the equivalent file is under `%APPDATA%\Zed\AGENTS.md`.

## Project Instructions {#project-instructions}

Project instruction files apply to the current project. Zed uses the first matching file in this list:

- `.rules`
- `.cursorrules`
- `.windsurfrules`
- `.clinerules`
- `.github/copilot-instructions.md`
- `AGENT.md`
- [`AGENTS.md`](https://agents.md/)
- `CLAUDE.md`
- `GEMINI.md`

Project instructions override personal `AGENTS.md` when they conflict.

## Instruction File Support {#support}

| File                              | Zed-managed instructions                               | External Agents       | Terminal Threads                 |
| --------------------------------- | ------------------------------------------------------ | --------------------- | -------------------------------- |
| `~/.config/zed/AGENTS.md`         | Loaded as personal instructions where supported         | Not generally used    | Not used unless the CLI reads it |
| Project `AGENTS.md`               | Loaded as project instructions where supported          | Depends on the agent  | Depends on the CLI               |
| `CLAUDE.md`                       | Compatible project instructions where supported         | Claude reads natively | Claude Code CLI reads natively   |
| `.github/copilot-instructions.md` | Compatible project instructions where supported         | Depends on the agent  | Depends on the CLI               |

External Agents and Terminal Threads may read their own native instruction files directly. Do not assume Zed's instruction loader controls those agents.

## Instructions vs. Skills {#instructions-vs-skills}

| Use          | Best for                | Example                                                     |
| ------------ | ----------------------- | ----------------------------------------------------------- |
| Instructions | Always-on guidance      | Repository conventions, preferred tone, project constraints |
| Skills       | Reusable task workflows | Code review checklist, release workflow, migration helper   |

## Migrating from Rules {#migrating-from-rules}

Rules have been replaced by Skills and Instructions:

- reusable, on-demand Rules become [Skills](./skills.md)
- default, always-on Rules become personal `AGENTS.md`
- project `.rules` files remain supported as compatibility project instruction files
