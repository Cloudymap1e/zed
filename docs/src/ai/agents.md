---
title: AI Agents in Zed
description: Compare External Agents and Terminal Threads.
---

# Agents

Zed supports two agent paths. Choose the path based on how you want agentic work to run.

| Agent path                                | Runs in                         | Uses                                             | Best when                                                                              |
| ----------------------------------------- | ------------------------------- | ------------------------------------------------ | -------------------------------------------------------------------------------------- |
| [External Agents](./external-agents.md)   | Agent Panel and Threads Sidebar | External Agent process and its own auth/config   | You want Claude, Codex, OpenCode, Copilot, Cursor, Pi, or another External Agent agent |
| [Terminal Threads](./terminal-threads.md) | Threads Sidebar and terminal    | Native CLI/TUI auth/config                       | You want the tool's command-line experience organized in Zed                           |

An agent path is sometimes called a harness. It is the way agentic work is started, displayed, configured, and controlled in Zed.

## Agent Path vs. LLM Provider {#agent-path-vs-llm-provider}

| Question                                  | Start here                          |
| ----------------------------------------- | ----------------------------------- |
| Which agent or CLI should run the work?   | This page                           |
| Which model should power Zed AI features? | [LLM Providers](./llm-providers.md) |

[External Agents](./external-agents.md) and [Terminal Threads](./terminal-threads.md) may use their own model setup. Zed-configured LLM providers power model-backed Zed AI features such as Inline Assistant, Git commit generation, and thread summaries.

## Thread Types {#thread-types}

Threads are the units shown in the [Threads Sidebar](./parallel-agents.md#threads-sidebar). Thread types include:

- [External Agent](./external-agents.md) threads
- [Terminal Threads](./terminal-threads.md)

Use [Parallel Agents](./parallel-agents.md) to run and manage multiple threads at once.
