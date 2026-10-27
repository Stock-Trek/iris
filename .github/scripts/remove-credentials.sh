#!/usr/bin/env bash
# Remove credentials from disk before handing control to the agent.
#
# pi runs with read/bash tools and is given untrusted issue and comment text,
# so any credential left on disk can be exfiltrated through a prompt injection.
# Call this after the key pi needs has been staged in its short-lived file and
# before pi starts.
set -u

# actions/checkout persists the GitHub token as a git credential. Remove it
# for any host, not just github.com.
while read -r key _; do
  git config --local --unset-all "$key" 2>/dev/null || true
done < <(git config --local --get-regexp '^http\..*\.extraheader$' 2>/dev/null || true)

# Remove SSH keys/config (for example a private crate registry key) and other
# common CLI credential stores that earlier steps may have written.
rm -rf \
  "${HOME}/.ssh" \
  "${HOME}/.aws" \
  "${HOME}/.kube" \
  "${HOME}/.config/gcloud" \
  "${HOME}/.docker"
rm -f \
  "${HOME}/.git-credentials" \
  "${HOME}/.netrc" \
  "${HOME}/.npmrc" \
  "${HOME}/.pypirc" \
  "${HOME}/.vault-token" \
  "${HOME}/.config/gh/hosts.yml" \
  "${HOME}/.cargo/credentials" \
  "${HOME}/.cargo/credentials.toml" \
  "${HOME}/.gem/credentials"
rm -rf \
  "${HOME}/.azure" \
  "${HOME}/.config/pip" \
  "${HOME}/.config/rclone" \
  "${HOME}/.terraform.d"

# Remove the runner's per-step command files. They can contain exported
# secrets and step outputs. Keep the file for the current step so the calling
# action can still publish its own outputs.
CMDS_DIR="${RUNNER_TEMP:-}/_runner_file_commands"
if [ -d "${CMDS_DIR}" ]; then
  for f in \
    "${CMDS_DIR}"/set_env_* \
    "${CMDS_DIR}"/set_output_* \
    "${CMDS_DIR}"/save_state_* \
    "${CMDS_DIR}"/add_path_* \
    "${CMDS_DIR}"/add_matcher_*; do
    case "$f" in
      "${GITHUB_ENV:-}"|"${GITHUB_OUTPUT:-}"|"${GITHUB_STATE:-}")
        continue
        ;;
    esac
    rm -f "$f"
  done
fi
