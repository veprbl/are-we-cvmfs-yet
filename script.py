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

def plot_lag(data: list, fqrn: str) -> None:
    """Plot synchronization lag from the state data for a specific FQRN.

    ``data`` should be a list of dictionaries with timestamp and fqrns data.
    The function saves the plot as ``lag_plot_{fqrn}.png``.
    """
    import matplotlib
    matplotlib.use('Agg')  # non‑interactive backend suitable for CI/headless
    import matplotlib.pyplot as plt
    plt.xkcd()
    plt.rcParams.update({'font.family': 'DejaVu Sans'})

    # Collect data for each host
    host_data = {}
    
    for entry in data:
        if not entry or 'timestamp' not in entry or 'fqrns' not in entry:
            continue
        
        # Skip entries that don't have data for this FQRN
        if fqrn not in entry['fqrns']:
            continue
            
        cur_ts = int(entry['timestamp'])
        current_time = datetime.datetime.utcfromtimestamp(cur_ts)
        
        for host, fetched_ts_str in entry['fqrns'][fqrn].items():
            if host not in host_data:
                host_data[host] = {'timestamps': [], 'lags': []}
            
            try:
                fetched_ts = int(fetched_ts_str)
                lag_hours = (fetched_ts - cur_ts) / 3600
                host_data[host]['timestamps'].append(current_time)
                host_data[host]['lags'].append(lag_hours)
            except (ValueError, TypeError):
                continue

    if host_data:
        plt.figure(figsize=(12, 6))
        ax = plt.gca()
        
        for host, data_dict in host_data.items():
            if data_dict['timestamps']:
                ax.plot(data_dict['timestamps'], data_dict['lags'], 
                       marker='o', label=host, alpha=0.7)
        
        ax.set_title(f'CVMFS Synchronization Lag by Host - {fqrn}')
        ax.set_xlabel('Date')
        ax.set_ylabel('Synchronization lag (hours)')
        ax.legend(bbox_to_anchor=(1.05, 1), loc='upper left')
        from matplotlib.dates import DateFormatter
        ax.xaxis.set_major_formatter(DateFormatter('%b %d'))
        ax.grid(True)
        plt.tight_layout()
        plt.savefig(f'lag_plot_{fqrn}.png', bbox_inches='tight')
        plt.show()
    else:
        print(f'No valid timestamp pairs found for plotting {fqrn}.')


def fetch_cvmfs_timestamp(host_url):
    """Fetch the .cvmfspublished file and return the UNIX timestamp following the leading 'T'."""
    resp = requests.get(host_url, timeout=10)
    if resp.status_code != 200:
        raise RuntimeError(f"Failed to fetch cvmfs timestamp from {host_url}: {resp.status_code}")
    for line in resp.text.splitlines():
        if line.startswith('T'):
            # line format: T<unix_timestamp>
            return line[1:].strip()
    raise RuntimeError(f"No line starting with 'T' found in .cvmfspublished from {host_url}")


def fetch_all_cvmfs_timestamps(fqrn):
    """Fetch timestamps from all CVMFS hosts for a given FQRN."""
    host_bases = [
        "http://cvmfs-egi.gridpp.rl.ac.uk:8000/cvmfs",
        "http://klei.nikhef.nl:8000/cvmfs", 
        "http://cvmfs-s1bnl.opensciencegrid.org:8000/cvmfs",
        "http://cvmfs-s1fnal.opensciencegrid.org:8000/cvmfs",
        "http://cvmfsrep.grid.sinica.edu.tw:8000/cvmfs",
        "http://cvmfs-stratum-one.ihep.ac.cn:8000/cvmfs"
    ]
    
    results = {}
    for host_base in host_bases:
        host_url = f"{host_base}/{fqrn}/.cvmfspublished"
        try:
            timestamp = fetch_cvmfs_timestamp(host_url)
            # Extract host identifier from URL
            host_name = host_base.split("://")[1].split(":")[0]
            results[host_name] = timestamp
        except Exception as e:
            print(f"Warning: Failed to fetch from {host_url}: {e}")
            # Continue with other hosts
    
    return results




def main():
    owner, repo = get_repo_info()
    token = os.getenv("GITHUB_TOKEN")
    if not token:
        raise RuntimeError("GITHUB_TOKEN not set")

    file_path = "state.json"
    branch = "state"
    fqrns = ["singularity", "eic"]

    # Try to read current JSON content
    try:
        content, sha = get_file(owner, repo, file_path, branch, token)
        data = json.loads(content)
        print(f"Current content of {file_path} loaded successfully")
    except Exception as e:
        print(f"Could not load {file_path}: {e}")
        data = []
        sha = None  # Will be a new file

    # Fetch timestamps from all CVMFS hosts for each FQRN
    fqrn_data = {}
    for fqrn in fqrns:
        host_timestamps = fetch_all_cvmfs_timestamps(fqrn)
        if host_timestamps:
            fqrn_data[fqrn] = host_timestamps
        else:
            print(f"Warning: Failed to fetch timestamps for FQRN {fqrn}")
    
    if not fqrn_data:
        raise RuntimeError("Failed to fetch timestamps from any FQRN")
    
    # Create new entry with current timestamp and all FQRN data
    current_timestamp = str(int(datetime.datetime.utcnow().timestamp()))
    new_entry = {
        "timestamp": current_timestamp,
        "fqrns": fqrn_data
    }
    
    # Append new entry to data
    data.append(new_entry)
    
    # Convert back to JSON string
    new_content = json.dumps(data, indent=2)
    
    # Update the file
    if sha:
        update_file(owner, repo, file_path, new_content, sha, branch, token)
    else:
        # Create new file (no SHA needed for new files)
        url = f"https://api.github.com/repos/{owner}/{repo}/contents/{file_path}"
        headers = {"Authorization": f"token {token}", "Accept": "application/vnd.github.v3+json"}
        payload = {
            "message": f"Create {file_path} via GitHub Action",
            "content": base64.b64encode(new_content.encode()).decode(),
            "branch": branch,
        }
        resp = requests.put(url, headers=headers, data=json.dumps(payload))
        if resp.status_code not in (200, 201):
            raise RuntimeError(f"Failed to create file: {resp.status_code} {resp.text}")
        print(f"Successfully created {file_path} on branch {branch}")

    # Visualization: plot synchronization lag over time for each FQRN
    for fqrn in fqrns:
        plot_lag(data, fqrn)

if __name__ == "__main__":
    main()
