
'''


from datasource.UtilGit import UtilGit

git_settings = {
    'repo_url': 'https://github.com/CrocSwap/sdk',
    'repo_branch': 'main',
    #'repo_user': 'user',
    #'repo_key': 'your_personal_access_token_or_ssh_key'
}
root_dir = './git_backup_test/'
repo_name = 'my_repo'

UtilGit.download_git_data(git_settings, root_dir, 'my_repo')
'''
from decelium_wallet.commands.BaseService import BaseService
class HelloCommand(BaseService):
    @classmethod
    def run(cls, **kwargs):
        print(f"MyService running with arguments: {kwargs}")
        return 0  # Success exit code

# Enable the CLI interface via "python3 HelloCommand.py arg1=arg arg2=arg_other"
if __name__ == "__main__": 
    HelloCommand.run_cli()