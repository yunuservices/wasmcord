use std::path::{Path, PathBuf};

use super::loader::plugin_dir;

pub(crate) fn workspace_path(name: &str) -> PathBuf {
    plugin_dir().join(name).join("workspace")
}

pub(crate) fn open_workspace(workspace: &Path) -> Result<cap_std::fs::Dir, String> {
    cap_std::fs::Dir::open_ambient_dir(workspace, cap_std::ambient_authority())
        .map_err(|e| format!("failed to open plugin workspace: {e}"))
}

pub(crate) fn workspace_read(workspace: &Path, path: &str) -> Result<Vec<u8>, String> {
    open_workspace(workspace)?
        .read(path)
        .map_err(|e| e.to_string())
}

pub(crate) fn workspace_write(workspace: &Path, path: &str, content: &[u8]) -> Result<(), String> {
    let dir = open_workspace(workspace)?;
    if let Some(parent) = Path::new(path).parent()
        && !parent.as_os_str().is_empty()
    {
        dir.create_dir_all(parent).map_err(|e| e.to_string())?;
    }
    dir.write(path, content).map_err(|e| e.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn workspace_rejects_absolute_path() {
        let dir = std::env::temp_dir().join("ynsrvcs-ws-abs");
        std::fs::create_dir_all(&dir).unwrap();
        assert!(workspace_read(&dir, "/etc/passwd").is_err());
        assert!(workspace_write(&dir, "/tmp/ynsrvcs-escape", b"x").is_err());
        assert!(!Path::new("/tmp/ynsrvcs-escape").exists());
    }

    #[test]
    fn workspace_rejects_parent_traversal() {
        let dir = std::env::temp_dir().join("ynsrvcs-ws-traversal");
        std::fs::create_dir_all(&dir).unwrap();
        assert!(workspace_read(&dir, "../../../../etc/passwd").is_err());
        assert!(workspace_write(&dir, "../ynsrvcs-escape", b"x").is_err());
        assert!(!dir.parent().unwrap().join("ynsrvcs-escape").exists());
    }

    #[test]
    fn workspace_allows_contained_paths() {
        let dir = std::env::temp_dir().join("ynsrvcs-ws-ok");
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).unwrap();
        workspace_write(&dir, "nested/state.json", b"{}").unwrap();
        assert_eq!(workspace_read(&dir, "nested/state.json").unwrap(), b"{}");
    }
}
