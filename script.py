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

def plot_lag(content_str: str) -> None:
    """Plot synchronization lag from the state file content.

    Each line of ``content_str`` should be ``<current_unix> <fetched_unix>``.
    The function saves the plot as ``lag_plot.png``.
    """
    import matplotlib
    matplotlib.use('Agg')  # non‑interactive backend suitable for CI/headless
    import matplotlib.pyplot as plt
    plt.xkcd()
    plt.rcParams.update({'font.family': 'DejaVu Sans'})

    timestamps = []
    lags = []
    for line in content_str.strip().split('\n'):
        parts = line.split()
        if len(parts) != 2:
            continue
        cur_ts = int(parts[0])
        fetched_ts_line = int(parts[1])
        timestamps.append(datetime.datetime.utcfromtimestamp(cur_ts))
        # convert lag from seconds to hours
        lags.append((fetched_ts_line - cur_ts) / 3600)

    if timestamps:
        plt.figure(figsize=(10, 5))
        ax = plt.gca()
        ax.plot(timestamps, lags, marker='o')
        ax.set_title('CVMFS Synchronization Lag')
        ax.set_xlabel('Date')
        ax.set_ylabel('Synchronization lag (hours)')
        from matplotlib.dates import DateFormatter
        ax.xaxis.set_major_formatter(DateFormatter('%b %d'))
        ax.grid(True)
        plt.tight_layout()
        plt.savefig('lag_plot.png')
        plt.show()
    else:
        print('No valid timestamp pairs found for plotting.')


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

    # Visualization: plot synchronization lag over time
    plot_lag(new_content)

if __name__ == "__main__":
    main()
