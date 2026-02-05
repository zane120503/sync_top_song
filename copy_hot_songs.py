import os
import requests
import paramiko
from dotenv import load_dotenv
from tqdm import tqdm
import sys

# Load environment variables
load_dotenv()

# SSH Configuration
SSH_HOST = os.getenv("SSH_HOST")
SSH_PORT = int(os.getenv("SSH_PORT", "22"))
SSH_USER = os.getenv("SSH_USER")
SSH_PASS = os.getenv("SSH_PASS")

# NAS Directories
NAS_SOURCE_DIR = os.getenv("NAS_SOURCE_DIR")
NAS_TARGET_DIR = os.getenv("NAS_TARGET_DIR")

API_URL = os.getenv("API_URL", "http://localhost:8000/top_songs")
LIMIT = os.getenv("limit", "200000")

def get_hot_song_ids():
    """Fetch top song IDs from the API."""
    try:
        print(f"Fetching hot songs from {API_URL}...")
        response = requests.get(API_URL, params={"limit": LIMIT})
        response.raise_for_status()
        data = response.json()
        song_ids = {item['song_id'] for item in data if item.get('song_id')}
        print(f"Found {len(song_ids)} hot songs from API.")
        return song_ids
    except requests.exceptions.RequestException as e:
        print(f"Error fetching data from API: {e}")
        return set()

def get_ssh_client():
    """Create and return an SSH client."""
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        print(f"Connecting to NAS at {SSH_HOST}...")
        client.connect(SSH_HOST, port=SSH_PORT, username=SSH_USER, password=SSH_PASS)
        return client
    except Exception as e:
        print(f"Failed to connect to NAS: {e}")
        return None

def copy_hot_songs_ssh():
    # Validate configuration
    if not all([SSH_HOST, SSH_USER, SSH_PASS, NAS_SOURCE_DIR, NAS_TARGET_DIR]):
        print("Error: Missing SSH or NAS configuration in .env")
        return

    ssh = get_ssh_client()
    if not ssh:
        return

    try:
        hot_song_ids = get_hot_song_ids()
        if not hot_song_ids:
            print("No song IDs found to copy.")
            return

        print(f"Scanning NAS source directory: {NAS_SOURCE_DIR}")
        
        # List directories in NAS source
        # Using 'ls -1' to get just filenames
        stdin, stdout, stderr = ssh.exec_command(f"ls -1 '{NAS_SOURCE_DIR}'")
        file_list = stdout.read().decode().splitlines()
        
        error_msg = stderr.read().decode()
        if error_msg:
             print(f"Error listing directory: {error_msg}")

        # Create target directory if it doesn't exist
        print(f"Ensuring target directory exists: {NAS_TARGET_DIR}")
        ssh.exec_command(f"mkdir -p '{NAS_TARGET_DIR}'")

        # Filter items
        to_copy = [item for item in file_list if item in hot_song_ids]
        print(f"Found {len(to_copy)} matching folders to copy.")
        
        copied_count = 0
        skipped_count = 0

        for item in tqdm(to_copy, desc="Copying on NAS"):
            source_path = f"{NAS_SOURCE_DIR}/{item}"
            target_path = f"{NAS_TARGET_DIR}/{item}"
            
            # Check if target already exists using 'test -d'
            check_cmd = f"test -d '{target_path}'"
            stdin, stdout, stderr = ssh.exec_command(check_cmd)
            exit_status = stdout.channel.recv_exit_status()
            
            if exit_status == 0:
                # print(f"Skipping {item} (already exists)")
                skipped_count += 1
                continue

            # Construct copy command (cp -r)
            # Using -r for recursive.
            cmd = f"cp -r '{source_path}' '{NAS_TARGET_DIR}/'"
            
            stdin, stdout, stderr = ssh.exec_command(cmd)
            exit_status = stdout.channel.recv_exit_status()
            
            if exit_status == 0:
                copied_count += 1
            else:
                err = stderr.read().decode()
                print(f"Error copying {item}: {err}") 
                # skipped_count += 1 # Already counted in skipped or failed? Let's track failures separately if needed but count is simple here.


        print(f"\nProcess completed.")
        print(f"Success: {copied_count}")
        print(f"Failed: {skipped_count}")

    except Exception as e:
        print(f"An error occurred: {e}")
    finally:
        ssh.close()
        print("SSH connection closed.")

if __name__ == "__main__":
    copy_hot_songs_ssh()
