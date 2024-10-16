import os
import shutil
import tempfile
import json
import subprocess
from GitBackupManager import GitBackupManager  # Assuming this is the correct class name
from decelium_wallet.commands.BaseService import BaseService  # Assuming your BaseService from earlier

class TestGitBackupManager(BaseService):
    @staticmethod
    def init(backup_root_path, test_output_path):
        """
        Initializes the necessary directories.
        """
        print("Initializing directories...")
        os.makedirs(backup_root_path, exist_ok=True)
        os.makedirs(test_output_path, exist_ok=True)
        print("Initialization complete.")

    @staticmethod
    def list_repositories(github_username, github_access_token, num_repos_to_test):
        print("Listing GitHub repositories...")
        repos_json = GitBackupManager.list_github_repos(github_username, github_access_token)
        assert not "error" in repos_json, f"Git Connection Error: {repos_json}"
        repos = json.loads(repos_json)
        num_repos_to_test = int(num_repos_to_test)
        assert len(repos) >= num_repos_to_test, f"Less than {num_repos_to_test} repositories found."

        #for i in range(num_repos_to_test):
        #    repo = repos[i]
        #    assert 'clone_url' in repo, f"Repository {repo['name']} does not have a clone URL."
        #    print(f"Repo {i+1}: {repo['name']} - {repo['clone_url']}")
        return repos

    @staticmethod
    def download_and_encrypt_repository(github_username, github_access_token, encryption_password,
                                        repo_name, clone_url, backup_root_path, default_branch):
        backup_path = os.path.join(backup_root_path, repo_name)

        print(f"Downloading and encrypting repository '{repo_name}'...")
        GitBackupManager.download(
            git_username=github_username,
            git_access_key=github_access_token,
            encryption_password=encryption_password,
            repo_url=clone_url,
            branch=default_branch,
            backup_path=backup_path
        )
        assert os.path.isdir(backup_path), "Encrypted backup was not created."
        print("Download and encryption successful.")
        return backup_path

    @staticmethod
    def decrypt_repository(repo_name, backup_path, encryption_password, test_output_path):
        decrypted_output_path = os.path.join(test_output_path, repo_name)
        print(f"Decrypting repository '{repo_name}'...")
        GitBackupManager.unpack(
            backup_path=backup_path,
            encryption_password=encryption_password,
            output_path=decrypted_output_path
        )
        assert os.path.isdir(decrypted_output_path), "Decrypted repository was not created."
        print("Decryption successful.")
        return decrypted_output_path

    @staticmethod
    def verify_repository(github_username, repo_name, clone_url, decrypted_path, default_branch):
        # Create a temporary directory for verification
        temp_dir = tempfile.mkdtemp()
        try:
            # Clone the original repository to a temporary location for comparison
            original_repo_path = os.path.join(temp_dir, f"{repo_name}_original")

            print(f"Cloning original repository '{repo_name}' for verification...")
            subprocess.run(
                ['git', 'clone', '--branch', default_branch, clone_url, original_repo_path],
                check=True
            )
            assert os.path.isdir(original_repo_path), "Failed to clone the original repository."
            print("Original repository cloned successfully.")

            # Compare the original repository with the decrypted one
            print("Verifying decrypted files match the original repository...")
            diff_cmd = ['diff', 
                        '-r',
                        '--exclude=.gitignore',
                        '--exclude=.enc.info',
                        '--exclude=dec.object.json',
                        '--exclude=.git',
                        original_repo_path, 
                        decrypted_path]
            diff_result = subprocess.run(diff_cmd, capture_output=True)
            diff_result = subprocess.run(diff_cmd, capture_output=True)
            diff_output = diff_result.stdout.decode('utf-8')
            diff_error = diff_result.stderr.decode('utf-8')

            if diff_result.returncode != 0:
                print("Differences found during verification:")
                print(diff_output)
                print(diff_error)
                raise AssertionError("Decrypted files do not match the original repository.")
            else:
                print("Verification successful.")            
            assert diff_result.returncode == 0, f"Decrypted files do not match the original repository.{original_repo_path}, {decrypted_path}"
            print("Verification successful.")
        finally:
            pass
            # Clean up the temporary directory
            #shutil.rmtree(temp_dir)
            #print("Temporary verification directory cleaned up.")

    @staticmethod
    def restore_repository(github_username_url,github_username, github_access_token, encryption_password, repo_name,
                           backup_path, default_branch):
        restored_repo_name = f"{repo_name}_restored"
        restored_repo_url = f"https://github.com/{github_username_url}/{restored_repo_name}.git"
        print(f"Creating restored repository '{restored_repo_name}' on GitHub...")

        # Create the new repository using GitHub API
        create_repo_cmd = [
            'curl', '-u', f'{github_username}:{github_access_token}',
            '-X', 'POST', 'https://api.github.com/user/repos',
            '-d', json.dumps({'name': restored_repo_name})
        ]
        create_repo_result = subprocess.run(create_repo_cmd, capture_output=True, text=True)
        #raise Exception(f"Should have created https://github.com/{github_username_url}/{restored_repo_name}.git")
        assert create_repo_result.returncode == 0, f"Failed to create repository on GitHub: {create_repo_result.stderr}"
        print("Restored repository created on GitHub.")

        # Restore the repository
        print(f"Restoring repository '{restored_repo_name}'...")
        GitBackupManager.restore_to(
            backup_path=backup_path,
            encryption_password=encryption_password,
            repo_url=restored_repo_url,
            branch=default_branch,
            git_username=github_username,
            git_access_key=github_access_token,
            commit_message='Restored backup'
        )
        print(f"Repository restored '{restored_repo_name}' successfully.")
        return restored_repo_name

    @staticmethod
    def confirm_repository_restored(github_username_url, github_access_token, restored_repo_name):
        print(f"Confirming the restored repository '{restored_repo_name}' exists on GitHub...")
        check_repo_cmd = [
            'curl', '-u', f'{github_username}:{github_access_token}',
            '-H', 'Accept: application/vnd.github.v3+json',
            f'https://api.github.com/repos/{github_username_url}/{restored_repo_name}'
        ]
        check_repo_result = subprocess.run(check_repo_cmd, capture_output=True, text=True)
        assert check_repo_result.returncode == 0, f"Failed to check repository on GitHub: {check_repo_result.stderr}"
        repo_info = json.loads(check_repo_result.stdout)
        assert 'name' in repo_info and repo_info['name'] == restored_repo_name, f"Restored repository not found on GitHub:{restored_repo_name}, {check_repo_result.stdout}"
        print("Restored repository confirmed on GitHub.")

    @staticmethod
    def run_tests(github_username_url,github_username, github_access_token, encryption_password, backup_root_path,
                  test_output_path, num_repos_to_test):
        try:
            # Initialize directories
            TestGitBackupManager.init(backup_root_path, test_output_path)

            # List repositories
            repos = TestGitBackupManager.list_repositories(
                github_username, github_access_token, num_repos_to_test
            )
            for repo in repos:
                print (repo)

            repos = [
{'name': 'testweb', 'html_url': 'https://github.com/JustinGirard/testweb', 'clone_url': 'https://github.com/JustinGirard/testweb.git', 'ssh_url': 'git@github.com:JustinGirard/testweb.git', 'branch': 'master'}
            ]
            for repo in repos:
                repo_name = repo['name']
                clone_url = repo['clone_url']
                print(f"\nTesting repository: {repo_name}")

                # Download and encrypt the repository
                backup_path = TestGitBackupManager.download_and_encrypt_repository(
                    github_username, github_access_token, encryption_password,
                    repo_name, clone_url, backup_root_path,repo['branch']
                )
                # Decrypt the repository
                decrypted_output_path = TestGitBackupManager.decrypt_repository(
                    repo_name, backup_path, encryption_password, test_output_path
                )

                # Verify the repository
                TestGitBackupManager.verify_repository(
                    github_username, repo_name, clone_url, decrypted_output_path, repo['branch']
                )

                # Restore the repository to GitHub
                restored_repo_name = TestGitBackupManager.restore_repository(
                    github_username_url, github_username, github_access_token, encryption_password,
                    repo_name, backup_path, repo['branch']
                )

                print(f"Confirming'{restored_repo_name}' is on Github.")
                # Confirm the repository is restored on GitHub
                TestGitBackupManager.confirm_repository_restored(
                    github_username_url, github_access_token, restored_repo_name
                )
                break
            print("\nAll tests completed successfully.")

        finally:
            print("\nTest run completed.")

    @classmethod
    def get_command_map(cls):
        return {
            'init': {
                'required_args': ['backup_root_path', 'test_output_path'],
                'method': cls.init,
            },
            'list_repositories': {
                'required_args': ['github_username', 'github_access_token', 'num_repos_to_test'],
                'method': cls.list_repositories,
            },
            'download_and_encrypt_repository': {
                'required_args': [
                    'github_username', 'github_access_token', 'encryption_password',
                    'repo_name', 'clone_url', 'backup_root_path', 'default_branch'
                ],
                'method': cls.download_and_encrypt_repository,
            },
            'decrypt_repository': {
                'required_args': [
                    'repo_name', 'backup_path', 'encryption_password', 'test_output_path'
                ],
                'method': cls.decrypt_repository,
            },
            'verify_repository': {
                'required_args': [
                    'github_username', 'repo_name', 'clone_url', 'decrypted_path', 'default_branch'
                ],
                'method': cls.verify_repository,
            },
            'restore_repository': {
                'required_args': [
                    'github_username', 'github_access_token', 'encryption_password',
                    'repo_name', 'backup_path', 'default_branch'
                ],
                'method': cls.restore_repository,
            },
            'confirm_repository_restored': {
                'required_args': [
                    'github_username', 'github_access_token', 'restored_repo_name'
                ],
                'method': cls.confirm_repository_restored,
            },
            'run_tests': {
                'required_args': [
                    'github_username', 'github_access_token', 'encryption_password',
                    'backup_root_path', 'test_output_path', 'num_repos_to_test', 'default_branch'
                ],
                'method': cls.run_tests,
            },
        }

if __name__ == "__main__":
    #TestGitBackupManager.run_cli()
    github_username = 'justin.girard'
    github_username_url = "JustinGirard"
    github_access_token =  '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'
    encryption_password = 'passpass'
    backup_root_path = './backup_test_path'
    test_output_path=  './backup_out_path'
    num_repos_to_test = 1
    TestGitBackupManager.run_tests(github_username_url,github_username, github_access_token, encryption_password, backup_root_path,
                    test_output_path, num_repos_to_test)

