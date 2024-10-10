import os
import shutil
import subprocess
import json
import hashlib
import requests
from decelium_wallet.commands.BaseService import BaseService  # Assuming your BaseService from earlier
import decelium_wallet.core as core


class UtilGit(BaseService):

    @classmethod
    def run(cls, **kwargs):
        #return kwargs
        # Define a command-to-arguments mapping
        command_map = {
            'example': {
                'required_args': [],
                'method': cls.example_command,
            },
            'list_github_repos': {
                'required_args': ['username', 'access_token'],
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
                'required_args':['username','access_key','branch','repo_url', 'download_path', 'download_dir'],
                'method': cls.download_git_data
            }
        }

        assert len(kwargs['__command']) == 1, "Exactly one command must be specified"
        cmd = kwargs['__command'][0]
        if cmd not in command_map:
            raise ValueError(f"Unknown command: {cmd}")

        command_info = command_map[cmd]
        required_args = command_info.get('required_args', [])
        optional_args = command_info.get('optional_args', [])
        method = command_info['method']
        for arg in required_args:
            assert arg in kwargs, f"Missing required argument: {arg} for command {cmd}"
        method_kwargs = {arg: kwargs[arg] for arg in required_args}

        # Add optional arguments if present in kwargs
        for arg in optional_args:
            if arg in kwargs:
                method_kwargs[arg] = kwargs[arg]

        # Call the method and return the result
        return method(**method_kwargs)    
    @staticmethod
    def example_command():
        return "I am the output"
    
    @staticmethod
    def list_github_repos_OLD(username, access_token):
        """List all repositories from a user's GitHub account using GitHub API and personal access token, handling pagination."""
        url = f"https://api.github.com/user/repos"
        headers = {
            "Authorization": f"token {access_token}"
        }

        repo_names = []

        try:
            while url:
                print('.',end="")
                response = requests.get(url, headers=headers, params={"per_page": 100})  # Fetch 100 repos per page
                response.raise_for_status()  # Raises HTTPError for bad responses
                repos = response.json()

                # Add the current page's repositories to the list
                repo_names.extend([repo['name'] for repo in repos])

                # Check for the 'Link' header to get the next page URL
                if 'next' in response.links:
                    url = response.links['next']['url']
                else:
                    url = None

            print(f"Total repositories for {username}: {len(repo_names)}")
            for repo_name in repo_names:
                print(f" - {repo_name}")

            return repo_names
        except requests.exceptions.RequestException as e:
            print(f"Error fetching repositories: {e}")
            raise RuntimeError(f"Failed to list repositories: {e}")
    
    @staticmethod
    def list_github_repos(username, access_token):
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
            result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            # Parse the output to get the commit hash
            return result.stdout.decode().split()[0]
        except subprocess.CalledProcessError as e:
            print(f"Error retrieving remote commit hash: {e.stderr.decode()}")
            raise RuntimeError(f"Failed to retrieve remote commit hash: {e.stderr.decode()}")

    @staticmethod
    def is_repo_up_to_date(target_path, repo_url, repo_branch, backup_file):
        """Check if the local repo is up to date with the latest commit on the remote."""
        # Check if object.json exists
        if not os.path.exists(backup_file):
            return False  # No backup file means no metadata, so we need to pull

        with open(backup_file, 'r') as f:
            metadata = json.load(f)
        
        # Get the latest commit hash from the remote
        remote_commit_hash = UtilGit.get_remote_commit_hash(repo_url, repo_branch)

        # Compare with the commit hash in object.json
        if 'latest_commit_hash' in metadata and metadata['latest_commit_hash'] == remote_commit_hash:
            return True  # The repo is up to date

        return False  # Repo is outdated and needs to be pulled

    
    @classmethod
    def download_git_data(cls, username,access_key,branch,repo_url, download_path, download_dir):
        # Unpack git settings
        repo_url = repo_url #git_settings['repo_url']
        repo_branch = branch #git_settings.get('repo_branch', 'main')
        repo_user = username #git_settings.get('repo_user')
        repo_key = access_key #git_settings.get('repo_key')
        target_path = os.path.join(download_path, download_dir)
        backup_file = os.path.join(download_path,download_dir, 'dec.object.json')

        if cls.is_repo_up_to_date(target_path, repo_url, repo_branch, backup_file):
            print(f"Repository {repo_url} (branch: {repo_branch}) is already up to date. No need to pull.")
            return        
        # Generate obj_id based on repo_url + branch (for consistency)
        obj_id = cls.generate_obj_id(repo_url, repo_branch)

        # Step 1: Verify and clean up conflicting old data
        cls.handle_existing_repo(target_path, backup_file, repo_url, repo_branch)

        # Step 2: Clone the repository into the target directory
        cls.clone_repository(repo_url, repo_branch, repo_user, repo_key, target_path)

        # Step 3: Verify the clone was successful
        cls.verify_clone_success(target_path)

        # Step 4: Save metadata to object.json
        cls.save_repo_metadata(backup_file, obj_id, repo_url, repo_branch,target_path)

        cls.add_to_gitignore(target_path, file_to_ignore="dec.object.json")

        print(f"Repository {repo_url} (branch: {repo_branch}) successfully cloned to {target_path}.")
    




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

    @staticmethod
    def get_latest_commit_hash(repo_path):
        """Get the latest commit hash from the current branch in the given repository."""
        command = ['git', '-C', repo_path, 'rev-parse', 'HEAD']
        try:
            result = subprocess.run(command, check=True, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
            return result.stdout.decode().strip()
        except subprocess.CalledProcessError as e:
            print(f"Error retrieving latest commit hash: {e.stderr.decode()}")
            raise RuntimeError(f"Failed to retrieve the latest commit hash: {e.stderr.decode()}")

    @staticmethod
    def save_repo_metadata(backup_file, obj_id, repo_url, repo_branch, target_path):
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
        with open(backup_file, 'w') as f:
            json.dump(backup_data, f, indent=4)



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
    def generate_obj_id(repo_url, repo_branch):
        """Generate a unique object ID using SHA256 hash of the repo_url and branch."""
        self_id = core.uuid_gen(repo_url+'/'+repo_branch)
        #self_id = "obj-"+str(uuid.uuid4())    
        sha = hashlib.sha256(f'{repo_url}:{repo_branch}'.encode()).hexdigest()
        return f"obj-{sha}"

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
    def clone_repository(repo_url, repo_branch, repo_user, repo_key, target_path):
        """Clone the repository into the target directory."""
        git_clone_command = UtilGit.build_git_clone_command(repo_url, repo_branch, repo_user, repo_key, target_path)
        UtilGit.run_command(git_clone_command)

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
    def verify_clone_success(target_path):
        """Verify that the clone was successful by inspecting key files in the repo."""
        git_dir = os.path.join(target_path, '.git')
        if not os.path.exists(git_dir):
            raise RuntimeError(f"Clone failed: .git directory not found in {target_path}.")
        
        # Optionally, check for specific files or contents to ensure the clone is valid
        print(f"Verification successful: {target_path} contains a .git directory.")
'''
if __name__ == "__main__":
    UtilGit.run_cli()  # Inherit CLI behavior from BaseService

# python3 UtilGit.py download_git_data branch=master repo_url=https://github.com/justingirard/beanleaf download_path='./git_backup_test/' download_dir='beanleaf' username=justin.girard access_key=TOKEN
# python3 UtilGit.py list_github_repos username=justin.girard access_token=TOKEN
# python3 UtilGit.py list_local_repos download_path='./git_backup_test/' username=justin.girard access_token=TOKEN
