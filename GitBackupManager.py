import warnings
warnings.filterwarnings("ignore")
import os
import shutil
import tempfile
import json
import subprocess
from typing import Dict, Optional
from decelium_wallet.commands.BaseService import BaseService  # Assuming your BaseService from earlier
from datasource.UtilCrypto import UtilCrypto
from datasource.UtilGit import UtilGit
'''
#from .type.BaseData import BaseData
class DTGHRepo(BaseData):
    def get_keys(self):
        required = {'name':lambda v:v, # (╯°□°)╯︵ ┻━┻ 
                    'user_context':dict,
                    'connection_settings':dict,
                    'backup_path':str,
                    'local_test_folder':str,
                    }
        return required,{}


class DTRepo(BaseData):
    def assert_has_schema():

    def get_keys(self):
        required = {'name':lambda v:v, # (╯°□°)╯︵ ┻━┻ 
                    'modified':dict,
                    'branch':dict,
                    'backup_path':str,
                    'local_test_folder':str,
                    }
        return required,{}
'''
class GitBackupManager(BaseService):

    @classmethod 
    def list_github_repos(cls,username,access_token):
        return UtilGit.list_github_repos(username, access_token)
    
    @classmethod
    def download(cls,
        git_username:str,
        git_access_key:str,
        encryption_password: str,
        repo_url: str,
        branch: str,
        backup_path: str
    ):
        """
        Download and encrypt a repository branch to a secure folder.

        :param credentials: Dictionary containing git credentials.
            Expected keys: 'username', 'access_token' or 'app_password'.
        :param encryption_password: Password for encryption.
        :param repo_url: URL of the repository to clone.
        :param branch: Branch of the repository to clone.
        :param backup_path: Path to the secure folder where the encrypted backup will be stored.

        """

        git_repo_dir = 'gitrepo'
        meta_data_dir = ''
        temp_dir = tempfile.mkdtemp()
        try:
            # Clone the repository using UtilGit
            UtilGit.download_git_data(
                username=git_username,
                access_key=git_access_key,
                branch=branch,
                repo_url=repo_url,
                download_path=temp_dir,
                download_dir=git_repo_dir,
                meta_data_dir=meta_data_dir
            )

            #Set SRC and DST Paths
            source_metadata_dir = temp_dir
            source_repo_dir = os.path.join(temp_dir, git_repo_dir)
            dest_metadata_dir = backup_path
            dest_data_dir = os.path.join(backup_path, git_repo_dir)

            # Copy Repo Data
            UtilCrypto.encrypt_directory(
                source_dir=source_repo_dir,
                #dest_dir=os.path.join(encrypted_dir,'gitrepo'),
                dest_dir=dest_data_dir,
                metadata_dir=dest_metadata_dir,
                key=encryption_password
            )

            # Copy Metadata
            src_entity_path = os.path.join(source_metadata_dir,"dec.object.json")
            dst_entity_path = os.path.join(dest_metadata_dir,"dec.object.json")
            shutil.copy(src=src_entity_path,dst=dst_entity_path)
            print(f"Moved src to dst {src_entity_path}  to {dst_entity_path}")
            print(f"Repository encrypted successfully to {dest_metadata_dir}")

        except Exception as e:
            import traceback as tb
            print(f"Error during download and encryption: {e}:{tb.format_exc()}")
            raise
        finally:
            # Clean up the temporary directory
            print("Finished. Debug TMP dir: "+temp_dir)
            #shutil.rmtree(temp_dir)

    @classmethod
    def unpack(cls,
        backup_path: str,
        encryption_password: str,
        output_path: str
    ):
        """
        Decrypt and extract a backup to a chosen folder.

        :param backup_path: Path to the encrypted backup.
        :param encryption_password: Password for decryption.
        :param output_path: Path where the decrypted backup will be extracted.
        """
        try:
            # Decrypt the backup using UtilCrypto
            UtilCrypto.decrypt_directory(
                source_dir=backup_path,
                dest_dir=output_path,
                key=encryption_password
            )
            print(f"Backup decrypted successfully to {output_path}")
        except Exception as e:
            print(f"Error during decryption: {e}")
            raise
    '''
    @classmethod
    def restore_to(cls,
        backup_path: str,
        encryption_password: str,
        repo_url: str,
        branch: str,
        git_username:str,
        git_access_key:str,
        commit_message: str = 'Restored backup'
    ):
        raise Exception("I THINK I AM OLD")
        """
        Decrypt and upload a backup to a specified git remote URL.

        :param backup_path: Path to the encrypted backup.
        :param encryption_password: Password for decryption.
        :param repo_url: Remote git repository URL where the backup will be uploaded.
        :param branch: Branch to which the backup will be uploaded.
        :param credentials: Dictionary containing git credentials.
            Expected keys: 'username', 'access_token' or 'app_password'.
        :param commit_message: Commit message for the restore operation.
        """
        temp_dir = tempfile.mkdtemp()
        try:
            # Decrypt the backup to a temporary directory
            decrypted_dir = os.path.join(temp_dir, 'decrypted_repo')
            UtilCrypto.decrypt_directory(
                source_dir=backup_path,
                dest_dir=decrypted_dir,
                key=encryption_password
            )

            # Initialize a new git repository
            #subprocess.run(['git', 'init'], cwd=decrypted_dir, check=True)
            #subprocess.run(['git', 'checkout', '-b', branch], cwd=decrypted_dir, check=True)
            #subprocess.run(['git', 'add', '.'], cwd=decrypted_dir, check=True)
            #subprocess.run(['git', 'commit', '-m', commit_message], cwd=decrypted_dir, check=True)#

            # Set remote origin
            subprocess.run(['git', 'remote', 'add', 'origin', repo_url], cwd=decrypted_dir, check=True)

            # Push to the remote repository
            # Include credentials in the remote URL if necessary
            username = git_username
            access_key = git_access_key
            if username and access_key:
                authenticated_repo_url = repo_url.replace('https://', f'https://{username}:{access_key}@')
            else:
                authenticated_repo_url = repo_url

            subprocess.run(['git', 'push', authenticated_repo_url, branch, '--force'], cwd=decrypted_dir, check=True)
            print(f"Backup restored and pushed to {repo_url} (branch: {branch})")

        except Exception as e:
            print(f"Error during restore: {e}")
            raise
        finally:
            # Clean up the temporary directory
            shutil.rmtree(temp_dir)
    '''
    @classmethod
    def list_backups(cls,backup_root_path: str):
        """
        List all current encrypted backup entries (directory names).

        :param backup_root_path: Path where encrypted backups are stored.
        :return: List of backup directory names.
        """
        try:
            backups = [
                name for name in os.listdir(backup_root_path)
                if os.path.isdir(os.path.join(backup_root_path, name))
            ]
            print("Available backups:")
            for backup in backups:
                print(f" - {backup}")
            return backups
        except Exception as e:
            print(f"Error listing backups: {e}")
            raise
    
    @classmethod
    def get_command_map(cls):
        command_map = {
            'download': {
                'required_args': ['git_username','git_access_key', 'encryption_password', 'repo_url', 'branch', 'backup_path'],
                'method': cls.download,
            },
            'unpack': {
                'required_args': ['backup_path', 'encryption_password', 'output_path'],
                'method': cls.unpack,
            },
            'restore_to': {
                'required_args': ['backup_path', 'encryption_password', 'repo_url', 'branch', 'git_username','git_access_key'],
                'optional_args': ['commit_message'],
                'method': cls.restore_to,
            },
            'list': {
                'required_args': ['backup_root_path'],
                'method': cls.list_backups,
            },
        }
        return command_map

    @classmethod
    def run_subprocess_command(cls,command, cwd=None):
        """
        Runs a subprocess command, captures stdout and stderr,
        asserts that the return code is zero,
        and prints stdout and stderr in case of errors.

        :param command: List of command arguments.
        :param cwd: Current working directory for the command.
        :return: The subprocess.CompletedProcess instance.
        """
        result = subprocess.run(
            command,
            cwd=cwd,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if result.returncode != 0:
            print(f"Command '{' '.join(command)}' failed with return code {result.returncode}")
            print(f"Standard Output:\n{result.stdout}")
            print(f"Standard Error:\n{result.stderr}")
            raise AssertionError(f"Command '{' '.join(command)}' failed with return code {result.returncode}")
        return result


    @classmethod
    def restore_to(cls,
        backup_path: str,
        encryption_password: str,
        repo_url: str,
        branch: str,
        git_username: str,
        git_access_key: str,
        commit_message: str = 'Restored backup'
    ):
        # raise Exception("AUTOMATED RESTORE IN DRAFT, and will not be implemented, as it is a security risk to users. Enable and use this code at your own risk.")
        """
        Decrypt and upload a backup to a new GitHub repository.

        :param backup_path: Path to the encrypted backup.
        :param encryption_password: Password for decryption.
        :param repo_name: Name of the new repository to create on GitHub.
        :param branch: Branch to which the backup will be uploaded.
        :param git_username: GitHub username.
        :param git_access_key: GitHub access token with repo permissions.
        :param commit_message: Commit message for the restore operation.
        """
        import requests  # Ensure that the requests module is imported

        temp_dir = tempfile.mkdtemp()
        try:
            # Decrypt the backup to a temporary directory
            decrypted_dir = os.path.join(temp_dir, 'decrypted_repo')
            UtilCrypto.decrypt_directory(
                source_dir=backup_path,
                dest_dir=decrypted_dir,
                key=encryption_password
            )

            # Verify that the decrypted directory is a Git repository
            git_dir = os.path.join(decrypted_dir, '.git')
            if not os.path.isdir(git_dir):
                raise Exception("The decrypted backup does not contain a .git directory.")
            '''
            subprocess.run(['git', 'remote', 'set-url', 'origin', repo_url], cwd=decrypted_dir, check=True)
            subprocess.run(['git', 'checkout', branch], cwd=decrypted_dir, check=True)
            authenticated_repo_url = repo_url.replace('https://', f'https://{git_username}:{git_access_key}@')
            subprocess.run(['git', 'push', authenticated_repo_url, branch, '--force'], cwd=decrypted_dir, check=True)
            '''

            # Set remote origin to the new repository
            result_set_url = cls.run_subprocess_command(['git', 'remote', 'set-url', 'origin', repo_url], cwd=decrypted_dir)
            # Assert that there is no error output
            assert result_set_url.stderr.strip() == '', f"Unexpected error output: {result_set_url.stderr}"
            print("Remote origin set successfully.")

            # Ensure the correct branch is checked out
            result_checkout = cls.run_subprocess_command(['git', 'checkout', branch], cwd=decrypted_dir)
            # Assert that the branch was switched or already on the branch
            expected_messages = [
                f"Switched to branch '{branch}'",
                f"Already on '{branch}'",
                f"Your branch is up to date with 'origin/{branch}'."
            ]
            if not any(msg in result_checkout.stdout for msg in expected_messages):
                raise AssertionError(f"Unexpected checkout output: {result_checkout.stdout}")
            print(f"Checked out branch '{branch}' successfully.")


            # Push to the remote repository
            authenticated_repo_url = repo_url.replace('https://', f'https://{git_username}:{git_access_key}@')

            result_push = cls.run_subprocess_command(['git', 'push', authenticated_repo_url, branch, '--force'], cwd=decrypted_dir)
            # Assert that the push was successful
            if "error" in result_push.stderr.lower() or "fatal" in result_push.stderr.lower():
                raise AssertionError(f"Push failed with error: {result_push.stderr}")
            print(f"Pushed to remote repository '{repo_url}' on branch '{branch}' successfully.")

            print(f"Backup restored and pushed to {repo_url} (branch: {branch})")

        except Exception as e:
            print(f"Error during restore: {e}")
            raise
        finally:
            # Clean up the temporary directory
            shutil.rmtree(temp_dir)

if __name__ == "__main__":
    GitBackupManager.run_cli()