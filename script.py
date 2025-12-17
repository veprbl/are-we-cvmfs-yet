import os
import base64
import json
import datetime
import sys
import requests


def get_repo_info():
    repo = os.getenv("GITHUB_REPOSITORY")
    if not repo:
        raise RuntimeError("GITHUB_REPOSITORY not set")
    owner, name = repo.split("/", 1)
    return owner, name


def get_file(owner, repo, path, ref, token):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    params = {"ref": ref}
    resp = requests.get(url, headers=headers, params=params)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to get file: {resp.status_code} {resp.text}")
    data = resp.json()
    content = base64.b64decode(data["content"]).decode()
    sha = data["sha"]
    return content, sha


def update_file(owner, repo, path, new_content, sha, branch, token):
    url = f"https://api.github.com/repos/{owner}/{repo}/contents/{path}"
    headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
    payload = {
        "message": f"Update {path} via GitHub Action",
        "content": base64.b64encode(new_content.encode()).decode(),
        "sha": sha,
        "branch": branch,
    }
    resp = requests.put(url, headers=headers, data=json.dumps(payload))
    if resp.status_code not in (200, 201):
        raise RuntimeError(f"Failed to update file: {resp.status_code} {resp.text}")
    print(f"Successfully updated {path} on branch {branch}")


def fetch_cvmfs_timestamp():
    """Fetch the .cvmfspublished file and return the UNIX timestamp following the leading 'T'."""
    url = "http://cvmfs-s1bnl.opensciencegrid.org:8000/cvmfs/singularity/.cvmfspublished"
    resp = requests.get(url, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to fetch cvmfs timestamp: {resp.status_code}")
    for line in resp.text.splitlines():
        if line.startswith('T'):
            # line format: T<unix_timestamp>
            return line[1:].strip()
    raise RuntimeError("No line starting with 'T' found in .cvmfspublished")


def main():
    owner, repo = get_repo_info()
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN not set")

    file_path = "state.txt"
    branch = "state"

    # Read current content
    content, sha = get_file(owner, repo, file_path, branch, token)
    print(f"Current content of {file_path}:\n{content}")

    # Fetch the published timestamp from CVMFS
    fetched_ts = fetch_cvmfs_timestamp()
    # Append both timestamps: <current UTC UNIX> <fetched UNIX>
    timestamp = str(int(datetime.datetime.utcnow().timestamp()))
    new_content = content + f"\n{timestamp} {fetched_ts}"

    update_file(owner, repo, file_path, new_content, sha, branch, token)

if __name__ == "__main__":
    main()
