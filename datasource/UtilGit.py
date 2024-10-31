import warnings
warnings.filterwarnings("ignore")

import os
import shutil
import subprocess
import json
import hashlib
import requests
from decelium_wallet.commands.BaseService import BaseService  # Assuming your BaseService from earlier
import decelium_wallet.core as core
import decelium_wallet.crypto as crypto
from decelium_wallet.datasources.FileCache import FileCache
import zipfile
from io import BytesIO

class UtilGit(BaseService):

    @classmethod
    def get_command_map(cls):
        command_map = {
            'example': {
                'required_args': [],
                'method': cls.example_command,
            },
            'list_local_repos': {
                'required_args': ['backup_directory'],
                'method': cls.list_local_repos
            },
            'list_github_repos': {
                # (cls, username, access_token, cache_file, max_cache_age_seconds=500,limit=5, offset=0):
                'required_args': ['username', 'access_token','limit','offset','cache_file'],
                'method': cls.list_github_repos
            },
            'list_bitbucket_repos': {
                'required_args': ['bitbucket_user', 'app_password'],
                'method': cls.list_bitbucket_repositories
            },
            'get_remote_commit_hash': {
                'required_args': ['repo_url', 'repo_branch'],
                'optional_args': ['repo_user', 'repo_key'],
                'method': cls.get_remote_commit_hash
            },
            'is_repo_up_to_date': {
                'required_args': ['target_path', 'repo_url', 'repo_branch', 'backup_file'],
                'method': cls.is_repo_up_to_date
            },
            'download_git_data': {
                'required_args':['username','access_key','branch','repo_url', 'download_path', 'download_dir','meta_data_dir'],
                'method': cls.download_git_data
            }
        }
        return command_map
    
    @staticmethod
    def get_latest_commit_hash(repo_path):
        """Get the latest commit hash from the current branch in the given repository."""
        command = ['git', '-C', repo_path, 'rev-parse', 'HEAD']
        try:
            print(f"Running command: {' '.join(command)}")
            result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return result.stdout.decode().strip()
        except subprocess.CalledProcessError as e:
            print(f"Error retrieving latest commit hash: {e.stderr.decode()}")
            raise RuntimeError(f"Failed to retrieve the latest commit hash: {e.stderr.decode()}")    
    
    @staticmethod
    def verify_clone_success(target_path):
        """Verify that the clone was successful by inspecting key files in the repo."""
        git_dir = os.path.join(target_path, '.git')
        if not os.path.exists(git_dir):
            raise RuntimeError(f"Clone failed: .git directory not found in {target_path}.")
        
        # Optionally, check for specific files or contents to ensure the clone is valid
        print(f"Verification successful: {target_path} contains a .git directory.")

    @staticmethod
    def example_command():
        return "I am the output"
    
    '''
    @staticmethod 
    def list_github_repos(username, access_token, cache_file, max_cache_age_seconds=500,limit=5, offset=0):
        # Initialize the cache
        
        fc = FileCache(cache_file, max_cache_age_seconds)

        @fc.cached
        def get_repos():
            return UtilGit.list_github_repos_inner(username, access_token,limit,offset)

        return get_repos()
/Users/computercomputer/justinops/propagator/propenv/bin/python3 UtilGit.py list_github_repos username='justin.girard' access_token='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' cache_file='/var/folders/gx/dvqcqxt10jnbsjvscmgff5cc0000gn/T/DefaultCompany/GitBackupUI/github_repos_cache2.json' limit='5' offset='175'
    
    '''
    @staticmethod
    def list_github_repos(username, access_token, cache_file, max_cache_age_seconds=60, limit=5, offset=0):
        """
        Fetch a list of GitHub repositories for a user, using caching to avoid redundant API calls.
        It returns a sublist of the cached data based on the given limit and offset.
        """
        # Initialize the cache
        fc = FileCache(cache_file, max_cache_age_seconds)

        # Fetch all repositories with a large limit, using cached data if available
        @fc.cached
        def get_all_repos():
            return UtilGit.list_github_repos_inner(username, access_token, limit=500, offset=0)

        # Load the full cached repository list
        all_repos_json = get_all_repos()
        all_repos = json.loads(all_repos_json)
        #print(all_repos)
        # Apply the requested offset and limit to return the sublist
        start_index = int(offset)
        end_index = int(offset) + int(limit)
        if start_index >= len(all_repos):  # If offset is beyond available repos
            return json.dumps([], indent=4)  # Return empty list
        return json.dumps(all_repos[start_index:end_index], indent=4)    

    @staticmethod
    def list_github_repos_inner(username, access_token, limit=5, offset=0):
        """List all repositories from a user's GitHub account, returning clean JSON output."""
        url = f"https://api.github.com/user/repos"
        headers = {
            "Authorization": f"token {access_token}"
        }
        limit = int(limit)
        offset = int(offset)
        repos_data = []  # List to store repo names and URLs

        try:
            while url:
                response = requests.get(url, headers=headers, params={"per_page": 100})  # Fetch 100 repos per page
                response.raise_for_status()  # Raises HTTPError for bad responses
                repos = response.json()

                # Extract repository URLs along with their names
                for repo in repos:
                    repo_info = {
                        'name': repo['name'],
                        'html_url': repo['html_url'],  # The URL for visiting the repo
                        'clone_url': repo['clone_url'],  # URL used for cloning the repo
                        'ssh_url': repo['ssh_url'],  # SSH URL for cloning
                        'branch': repo['default_branch']   # Default branch of the repository                        
                    }
                    repos_data.append(repo_info)

                # Check for the 'next' URL for pagination
                if 'next' in response.links:
                    url = response.links['next']['url']
                else:
                    url = None

            # Apply offset and limit
            if offset >= len(repos_data):  # If offset is beyond available repos
                return json.dumps([], indent=4)  # Return empty list

            # Return the sliced list based on limit and offset
            return json.dumps(repos_data[offset:offset + limit], indent=4)

        except requests.exceptions.RequestException as e:
            error_message = {"error": f"Failed to list repositories: {str(e)}"}
            return json.dumps(error_message, indent=4)

        except Exception as e:
            # Return any other error with a traceback
            import traceback as tb
            error_traceback = tb.format_exc()
            error_message = {"error": error_traceback}
            return json.dumps(error_message, indent=4)

    @staticmethod
    def list_local_repos(backup_directory):
        """List all repositories from a local backup directory, returning clean JSON output."""
        repos_data = []  # List to store repo dictionaries

        # Iterate over each directory in the backup_directory
        for directory_name in os.listdir(backup_directory):
            dir_path = os.path.join(backup_directory, directory_name)

            # Check if it's a directory
            if os.path.isdir(dir_path):
                repo_dict = {'directory_name': directory_name}

                # Path to dec.object.json
                dec_object_path = os.path.join(dir_path, 'dec.object.json')

                # Check if dec.object.json exists
                if os.path.exists(dec_object_path):
                    try:
                        with open(dec_object_path, 'r') as dec_file:
                            nested_json = json.load(dec_file)
                            repo_dict['object'] = nested_json
                    except Exception as e:
                        # Handle any error while reading or parsing the file
                        repo_dict['object'] = {'error': f'Error reading dec.object.json: {str(e)}'}
                else:
                    # If dec.object.json is missing
                    repo_dict['object'] = {'error': 'missing dec.object.json'}

                # Append the repo dictionary to the list
                repos_data.append(repo_dict)

        # Return clean JSON output
        return json.dumps(repos_data, indent=4)
    
    @staticmethod
    def run_command(command):
        """Run a system command and handle exceptions."""
        try:
            print(f"Running command: {' '.join(command)}")
            result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            print(result.stdout.decode())
        except subprocess.CalledProcessError as e:
            print(f"Error executing command: {e.stderr.decode()}")
            raise RuntimeError(f"Git clone failed: {e.stderr.decode()}")

    @staticmethod
    def handle_existing_repo(target_path, backup_file, repo_url, repo_branch):
        """Back up or clean existing data, verifying the repo and branch match."""
        if os.path.exists(target_path):
            if os.path.exists(backup_file):
                with open(backup_file, 'r') as f:
                    previous_data = json.load(f)
                # Raise an error if the repo or branch don't match
                if previous_data['repo_url'] != repo_url or previous_data['repo_branch'] != repo_branch:
                    raise ValueError("Target directory contains a different repo or branch. Aborting.")
                else:
                    print("Overwriting existing repository...")
                    shutil.rmtree(target_path)  # Clean up old repo data
            else:
                print(f"Cleaning up contents at {target_path}. No metadata found.")
                shutil.rmtree(target_path)  # Clean up directory without metadata.

        os.makedirs(target_path, exist_ok=True)



    @staticmethod
    def build_git_clone_command(repo_url, repo_branch, repo_user, repo_key, target_path):
        """Build the appropriate git clone command, with or without authentication."""
        if repo_user:
            # HTTP clone with username
            authenticated_url = repo_url.replace("https://", f"https://{repo_user}:{repo_key}@")
            git_clone_command = [
                'git', 'clone', '--branch', repo_branch, authenticated_url, target_path
            ]
        elif repo_key:
            # Use SSH key for cloning
            git_clone_command = [
                'git', 'clone', '--branch', repo_branch, repo_url, target_path
            ]
        else:
            # Public repository
            git_clone_command = [
                'git', 'clone', '--branch', repo_branch, repo_url, target_path
            ]
        
        return git_clone_command        
    @staticmethod
    def list_bitbucket_repositories(bitbucket_user, app_password):
        """List all repositories from a user's Bitbucket account using Bitbucket API and app password."""
        url = f"https://api.bitbucket.org/2.0/repositories/{bitbucket_user}"
        auth = (bitbucket_user, app_password)  # Basic Authentication

        try:
            response = requests.get(url, auth=auth)
            response.raise_for_status()  # Raises HTTPError for bad responses
            repos = response.json()

            repo_names = [repo['name'] for repo in repos['values']]
            print(f"Repositories for {bitbucket_user}:")
            for repo_name in repo_names:
                print(f" - {repo_name}")

            return repo_names
        except requests.exceptions.RequestException as e:
            print(f"Error fetching repositories: {e}")
            raise RuntimeError(f"Failed to list repositories: {e}")
    
    @staticmethod
    def get_remote_commit_hash(repo_url, repo_branch, repo_user=None, repo_key=None):
        """Get the latest commit hash from the remote branch without cloning."""
        # Construct the correct git ls-remote command
        if repo_user and repo_key:
            # If using basic authentication, modify the URL with user credentials
            authenticated_url = repo_url.replace("https://", f"https://{repo_user}:{repo_key}@")
        else:
            authenticated_url = repo_url
        
        command = ['git', 'ls-remote', authenticated_url, repo_branch]
        try:
            print(f"Running command: {' '.join(command)}")
            result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # Parse the output to get the commit hash
            return result.stdout.decode().split()[0]
        except subprocess.CalledProcessError as e:
            print(f"Error retrieving remote commit hash: {e.stderr.decode()}")
            raise RuntimeError(f"Failed to retrieve remote commit hash: {e.stderr.decode()}")
    @staticmethod
    def generate_obj_id(repo_url, repo_branch):
        """Generate a unique object ID using SHA256 hash of the repo_url and branch."""
        self_id = core.uuid_gen(repo_url+'/'+repo_branch)
        #self_id = "obj-"+str(uuid.uuid4())    
        sha = hashlib.sha256(f'{repo_url}:{repo_branch}'.encode()).hexdigest()
        return f"obj-{sha}"

    @staticmethod
    def is_repo_up_to_date(meta_data_file, repo_url, repo_branch, backup_file):
        """Check if the local repo is up to date with the latest commit on the remote."""
        # Check if object.json exists
        if not os.path.exists(meta_data_file):
            return False  # No backup file means no metadata, so we need to pull

        with open(meta_data_file, 'r') as f:
            metadata = json.load(f)
        
        # Get the latest commit hash from the remote
        remote_commit_hash = UtilGit.get_remote_commit_hash(repo_url, repo_branch)

        # Compare with the commit hash in object.json
        if 'latest_commit_hash' in metadata and metadata['latest_commit_hash'] == remote_commit_hash:
            return True  # The repo is up to date

        return False  # Repo is outdated and needs to be pulled
    
    @staticmethod
    def clone_repository(repo_url, repo_branch, repo_user, repo_key, target_path):
        """Clone the repository into the target directory."""
        git_clone_command = UtilGit.build_git_clone_command(repo_url, repo_branch, repo_user, repo_key, target_path)
        UtilGit.run_command(git_clone_command)



    @staticmethod
    def clone_curl(repo_url, repo_branch,repo_user, access_token=None, target_path=None):
        """
        Clone a GitHub repository using curl by downloading the repo as a ZIP file and extracting it.
        :param repo_url: The URL of the repository to download.
        :param repo_branch: The branch to download (e.g., 'main' or 'master').
        :param access_token: Optional GitHub access token for private repos.
        :param target_path: The directory where the repo should be extracted.
        """
        # Prepare the URL for the ZIP download
        repo_name = repo_url.split("/")[-1]  # Extracts the repo name from the URL
        zip_url = f"{repo_url}/archive/refs/heads/{repo_branch}.zip"

        # Add Authorization header if access_token is provided (for private repos)
        headers = {}
        if access_token:
            headers['Authorization'] = f"token {access_token}"

        # Download the ZIP fil
        try:
            print(f"Downloading repository from {zip_url}")
            response = requests.get(zip_url, headers=headers, stream=True)
            response.raise_for_status()

            # Extract ZIP file contents to the target path
            with zipfile.ZipFile(BytesIO(response.content)) as zip_ref:
                # Extract all files into target path
                zip_ref.extractall(target_path)
            # Move the contents of the subdirectory to the target path
            top_level_dir = next(os.scandir(target_path)).path  # Get the path of the top-level extracted directory
            for item in os.listdir(top_level_dir):
                shutil.move(os.path.join(top_level_dir, item), target_path)                
            shutil.rmtree(top_level_dir)
            os.makedirs(os.path.join(target_path,'.git'), exist_ok=True)

            print(f"Repository downloaded and extracted to: {target_path}")
        except requests.exceptions.RequestException as e:
            print(f"Error downloading repository: {str(e)}")
            raise RuntimeError(f"Failed to download the repository: {str(e)}")
        except zipfile.BadZipFile as e:
            print(f"Error extracting ZIP file: {str(e)}")
            raise RuntimeError(f"Failed to extract ZIP file: {str(e)}")        

    @classmethod
    def add_to_gitignore(cls,target_path, file_to_ignore="object.json"):
        """Add the specified file to the .gitignore if it's not already present."""
        gitignore_path = os.path.join(target_path, '.gitignore')

        # Check if .gitignore exists; if not, create it.
        if not os.path.exists(gitignore_path):
            with open(gitignore_path, 'w') as f:
                f.write(f"{file_to_ignore}\n")
        else:
            # Append to .gitignore if the file is not already ignored.
            with open(gitignore_path, 'r+') as f:
                gitignore_content = f.read()
                if file_to_ignore not in gitignore_content:
                    f.write(f"\n{file_to_ignore}\n")
    @staticmethod
    def save_repo_metadata(metadata_path, obj_id, repo_url, repo_branch, target_path):
        """Save repository metadata (URL, branch, latest commit hash) into the object.json file."""
        # Retrieve the latest commit hash
        latest_commit_hash = UtilGit.get_latest_commit_hash(target_path)

        # Prepare backup data with latest commit hash
        backup_data = {
            'obj_id': obj_id,
            'repo_url': repo_url,
            'repo_branch': repo_branch,
            'latest_commit_hash': latest_commit_hash
        }

        # Save metadata to object.json
        with open(metadata_path, 'w') as f:
            json.dump(backup_data, f, indent=4)
    
    @classmethod
    def download_git_data(cls, username,access_key,branch,repo_url, download_path, download_dir,meta_data_dir):
        # Unpack git settings
        repo_url = repo_url #git_settings['repo_url']
        repo_branch = branch #git_settings.get('repo_branch', 'main')
        repo_user = username #git_settings.get('repo_user')
        repo_key = access_key #git_settings.get('repo_key')
        target_data_path = os.path.join(download_path, download_dir)
        target_metadata_path = os.path.join(download_path, meta_data_dir)
        target_metadata_file_path = os.path.join(target_metadata_path, 'dec.object.json')

        if cls.is_repo_up_to_date(target_metadata_file_path, repo_url, repo_branch, target_data_path):
            print(f"Repository {repo_url} (branch: {repo_branch}) is already up to date. No need to pull.")
            return        
        # Generate obj_id based on repo_url + branch (for consistency)
        obj_id = cls.generate_obj_id(repo_url, repo_branch)

        # Step 1: Verify and clean up conflicting old data
        cls.handle_existing_repo(target_data_path, target_metadata_file_path, repo_url, repo_branch)

        # Step 2: Clone the repository into the target directory
        #cls.clone_repository(repo_url, repo_branch, repo_user, repo_key, target_data_path)
        cls.clone_curl(repo_url, repo_branch, repo_user, repo_key, target_data_path)

        # Step 3: Verify the clone was successful
        cls.verify_clone_success(target_data_path)

        # Step 4: Save metadata to object.json
        print(f"Saving metadata to path: {target_metadata_file_path} ")
        cls.save_repo_metadata(target_metadata_file_path, obj_id, repo_url, repo_branch,target_data_path)

        #cls.add_to_gitignore(target_path, file_to_ignore="dec.DISABLED.object.json")
        #cls.add_to_gitignore(target_path, file_to_ignore=".enc.DISABLED.info")

        print(f"Repository {repo_url} (branch: {repo_branch}) successfully cloned to {target_data_path}.")
    



#
#
#
#
#
#
#
#
'''
class UtilGit(BaseService):
    
    @staticmethod
    def list_github_repositories(github_user, access_token):
        """List all repositories from a user's GitHub account, returning clean JSON output."""
        url = f"https://api.github.com/user/repos"
        headers = {
            "Authorization": f"token {access_token}"
        }

        repos_data = []  # List to store repo names and URLs

        try:
            while url:
                response = requests.get(url, headers=headers, params={"per_page": 100})  # Fetch 100 repos per page
                response.raise_for_status()  # Raises HTTPError for bad responses
                repos = response.json()

                # Extract repository URLs along with their names
                for repo in repos:
                    repo_info = {
                        'name': repo['name'],
                        'html_url': repo['html_url'],  # The URL for visiting the repo
                        'clone_url': repo['clone_url'],  # URL used for cloning the repo
                        'ssh_url': repo['ssh_url']  # SSH URL for cloning
                    }
                    repos_data.append(repo_info)

                # Check for the 'next' URL for pagination
                if 'next' in response.links:
                    url = response.links['next']['url']
                else:
                    url = None

            # Return clean JSON output
            return json.dumps(repos_data, indent=4)

        except requests.exceptions.RequestException as e:
            error_message = {"error": f"Failed to list repositories: {str(e)}"}
            return json.dumps(error_message, indent=4)

        except Exception as e:
            # Return any other error with a traceback
            error_traceback = traceback.format_exc()
            error_message = {"error": error_traceback}
            return json.dumps(error_message, indent=4)
    

    #@staticmethod
    #def save_repo_metadata(backup_file, obj_id, repo_url, repo_branch):
    #    """Save repository metadata (URL, branch) into the object.json file."""
    #    backup_data = {
    #        'obj_id': obj_id,
    #        'repo_url': repo_url,
    #        'repo_branch': repo_branch
    #    }
    #    with open(backup_file, 'w') as f:
    #        json.dump(backup_data, f, indent=4)













'''
if __name__ == "__main__":
    UtilGit.run_cli()  # Inherit CLI behavior from BaseService

# 'username', 'access_token','limit','offset','cache_file'
# python3 UtilGit.py list_github_repos username=justin.girard  limit=10 offset=0 cache_file='./temp/cache.dat' access_token=TOKEN 

# python3 UtilGit.py list_local_repos backup_directory='./git_backup_test/' username=justin.girard access_token=TOKEN
# python3 UtilGit.py download_git_data branch=master repo_url=https://github.com/justingirard/beanleaf download_path='./beandownload/' download_dir='beanleaf' username=justin.girard meta_data_dir='' access_key=TOKEN
# python3 UtilGit.py download_git_data branch=master repo_url=https://github.com/justingirard/beanleaf download_path='./beandownload_curl/' download_dir='beanleaf' username=justin.girard meta_data_dir='' access_key=TOKEN