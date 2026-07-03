mod db;
mod legacy_thread;
pub mod outline;
mod pattern_extraction;
mod sandboxing;
mod templates;
#[cfg(test)]
mod tests;
mod thread;
mod thread_store;
mod tool_permissions;
mod tools;

pub use db::*;
pub use pattern_extraction::*;
pub use sandboxing::{
    ThreadSandbox, sandbox_worktree_writable_paths, settings_sandbox_policy,
    settings_thread_sandbox,
};
pub use shell_command_parser::extract_commands;
pub use templates::*;
pub use thread::*;
pub use thread_store::*;
pub use tool_permissions::*;
pub use tools::*;

use agent_skills::{
    AGENTS_DIR_NAME, Skill, SkillSource, project_skills_relative_path, read_skill_body_from_content,
};
use anyhow::{Context as _, Result};
use chrono::{DateTime, Utc};
use fs::Fs;
use gpui::{AppContext, AsyncApp, Entity, EntityId, SharedString, Task};
use project::{
    AgentId, Project, REMOVED_BUILT_IN_AGENT_ID as REMOVED_BUILT_IN_AGENT_ID_STR, Worktree,
    WorktreeId,
};
use serde::{Deserialize, Serialize};
use std::path::PathBuf;
use std::sync::{Arc, LazyLock};
use util::rel_path::RelPath;

#[derive(Debug, Clone, Serialize, Deserialize, PartialEq)]
pub struct ProjectSnapshot {
    pub worktree_snapshots: Vec<project::telemetry_snapshot::TelemetryWorktreeSnapshot>,
    pub timestamp: DateTime<Utc>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum SkillLoadingIssueKind {
    LoadFailed,
    DescriptionTooLong,
    CatalogBudgetExceeded,
}

#[derive(Clone, Debug, PartialEq, Eq, Hash)]
pub struct SkillLoadingIssue {
    pub project_id: EntityId,
    pub path: PathBuf,
    pub message: SharedString,
    pub kind: SkillLoadingIssueKind,
}

pub const COMPACT_COMMAND_NAME: &str = "compact";

static SKILLS_PREFIX: LazyLock<Option<Arc<RelPath>>> = LazyLock::new(|| {
    RelPath::unix(project_skills_relative_path())
        .ok()
        .map(|path| path.into_arc())
});

async fn expand_worktree_directory(
    worktree: &Entity<Worktree>,
    path: &RelPath,
    cx: &mut AsyncApp,
) -> Result<()> {
    let expand_task = worktree.update(cx, |worktree, cx| {
        let entry_id = worktree
            .entry_for_path(path)
            .filter(|entry| entry.is_dir())
            .map(|entry| entry.id);
        entry_id.and_then(|entry_id| worktree.expand_entry(entry_id, cx))
    });

    if let Some(expand_task) = expand_task {
        expand_task.await?;
    }

    Ok(())
}

async fn expand_project_skills_directories(
    worktree: &Entity<Worktree>,
    cx: &mut AsyncApp,
) -> Result<()> {
    let agents_dir = RelPath::unix(AGENTS_DIR_NAME)?;
    let Some(skills_prefix) = SKILLS_PREFIX.as_ref() else {
        return Ok(());
    };

    expand_worktree_directory(worktree, agents_dir, cx).await?;
    expand_worktree_directory(worktree, skills_prefix, cx).await?;

    let skill_dirs = worktree.update(cx, |worktree, _cx| {
        worktree
            .child_entries(skills_prefix)
            .filter(|entry| entry.is_dir())
            .map(|entry| entry.path.clone())
            .collect::<Vec<_>>()
    });
    for skill_dir in skill_dirs {
        expand_worktree_directory(worktree, &skill_dir, cx).await?;
    }

    Ok(())
}

pub static REMOVED_BUILT_IN_AGENT_ID: LazyLock<AgentId> =
    LazyLock::new(|| AgentId::new(REMOVED_BUILT_IN_AGENT_ID_STR));

pub fn skill_body_resolver_for_project(
    project: Entity<Project>,
    fs: Arc<dyn Fs>,
) -> impl Fn(Skill, &mut AsyncApp) -> Task<Result<String>> + Send + Sync + 'static {
    move |skill, cx| match skill.source.clone() {
        SkillSource::ProjectLocal { worktree_id, .. } => {
            let project = project.clone();
            cx.spawn(async move |cx| {
                let worktree_id = WorktreeId::from_usize(worktree_id.0);
                let worktree = project
                    .update(cx, |project, cx| project.worktree_for_id(worktree_id, cx))
                    .context("no such worktree")?;
                expand_project_skills_directories(&worktree, cx).await?;
                let relative_path = worktree.update(cx, |worktree, _cx| {
                    let worktree_root = worktree.abs_path();
                    worktree
                        .path_style()
                        .strip_prefix(&skill.skill_file_path, &worktree_root)
                        .map(|relative_path| relative_path.into_arc())
                        .context("skill file is not inside its worktree")
                })?;

                let buffer = project
                    .update(cx, |project, cx| {
                        project.open_buffer((worktree_id, relative_path), cx)
                    })
                    .await?;
                let content =
                    cx.update(|cx| buffer.read(cx).as_text_snapshot().as_rope().to_string());

                read_skill_body_from_content(&skill.skill_file_path, &content).map_err(Into::into)
            })
        }
        SkillSource::BuiltIn | SkillSource::Global => {
            let fs = fs.clone();
            cx.background_spawn(async move {
                agent_skills::read_skill_body(fs.as_ref(), &skill.skill_file_path)
                    .await
                    .map_err(Into::into)
            })
        }
    }
}
