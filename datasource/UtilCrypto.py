import os
import json, base64
import hashlib
from decelium_wallet.commands.BaseService import BaseService  # Assuming your BaseService from earlier
import decelium_wallet.crypto as crypto

class UtilCrypto(BaseService):

    @staticmethod
    def compute_file_hash(file_path: str) -> str:
        """
        Compute the SHA-256 hash of a file.

        :param file_path: The path to the file.
        :return: The hexadecimal hash string.
        """
        sha256_hash = hashlib.sha256()
        with open(file_path, 'rb') as f:
            # Read and update hash string value in blocks of 4K
            for byte_block in iter(lambda: f.read(4096), b''):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()      
    '''
    @classmethod
    def run(cls, **kwargs):
        """
        Entry point for CLI integration, parsing commands and arguments.
        """
        # Define a command-to-arguments mapping
        command_map = {
            'encrypt_directory': {
                'required_args': ['source_dir', 'dest_dir', 'key'],
                'method': cls.encrypt_directory,
            },
            'decrypt_directory': {
                'required_args': ['source_dir', 'dest_dir', 'key'],
                'method': cls.decrypt_directory,
            },
        }

        # Ensure a command is specified
        assert '__command' in kwargs, "No command specified"
        assert len(kwargs['__command']) == 1, "Exactly one command must be specified"

        cmd = kwargs['__command'][0]
        if cmd not in command_map:
            raise ValueError(f"Unknown command: {cmd}")

        command_info = command_map[cmd]
        required_args = command_info.get('required_args', [])
        optional_args = command_info.get('optional_args', [])
        method = command_info['method']

        # Check for required arguments
        for arg in required_args:
            assert arg in kwargs, f"Missing required argument: {arg} for command {cmd}"

        # Prepare method arguments
        method_kwargs = {}
        for arg in required_args:
            if arg == 'key':
                # Build key_info dictionary from the key argument
                key_info = {'key': kwargs['key']}
                method_kwargs['key_info'] = key_info
            else:
                method_kwargs[arg] = kwargs[arg]

        # Add optional arguments if present in kwargs
        for arg in optional_args:
            if arg in kwargs:
                method_kwargs[arg] = kwargs[arg]

        # Call the method and return the result
        return method(**method_kwargs)
    '''
    @classmethod
    def get_command_map(cls):
        return {
            'encrypt_directory': {
                'required_args': ['source_dir', 'dest_dir', 'key'],
                'method': cls.encrypt_directory,
            },
            'decrypt_directory': {
                'required_args': ['source_dir', 'dest_dir', 'key'],
                'method': cls.decrypt_directory,
            },
            'get_file_cid': {
                'required_args': ['file_path'],
                'method': cls.get_file_cid,
            },
        }    

    @staticmethod
    def encrypt_file(src_file: str, key: str, dest_file: str) -> bool:
        """
        Encrypts a single file using the crypto.encode method.
        Converts file content to base64 encoded string before encryption.

        :param src_file: The path to the source file to encrypt.
        :param key_info: A dictionary containing key information for encryption.
        :param dest_file: The path where the encrypted file will be saved.
        :return: True if encryption was successful, False otherwise.
        """
        try:
            # Read the content of the source file in binary mode
            with open(src_file, 'rb') as f:
                content = f.read()

            # Convert the binary content to a base64 encoded string
            content_base64 = base64.b64encode(content).decode('utf-8')

            # Get the encryption password from key_info
            password = key

            # Encrypt the base64 string using crypto.encode
            encrypted_content = crypto.encode(content_base64, password)

            # Write the encrypted content to the destination file in text mode
            with open(dest_file, 'w', encoding='utf-8') as f:
                f.write(encrypted_content)

            return True
        except Exception as e:
            import traceback as tb
            tb.print_exc()
            print(f"Error encrypting file {src_file}: {e}")
            return False    
  
    @staticmethod
    def encrypt_directory(source_dir: str, dest_dir: str, key: str):
        """
        Encrypt all files in the source directory, record their hashes, and save the information in .enc.info.

        :param source_dir: The path to the source directory.
        :param dest_dir: The path to the destination directory.
        :param key_info: A dictionary containing key information for encryption.
        """
        file_hashes = {}
        enc_info_file = os.path.join(dest_dir, '.enc.info')

        for root, dirs, files in os.walk(source_dir):
            for file in files:
                # Construct the full path to the source file
                src_file = os.path.join(root, file)

                # Determine the relative path of the source file with respect to the source directory
                rel_path = os.path.relpath(src_file, source_dir)

                # Construct the full path to the destination file
                dest_file = os.path.join(dest_dir, rel_path)

                # Ensure the destination directory exists
                dest_dir_path = os.path.dirname(dest_file)
                os.makedirs(dest_dir_path, exist_ok=True)

                # Compute the hash of the original file
                file_hash = UtilCrypto.compute_file_hash(src_file)
                file_hashes[rel_path] = file_hash

                # Encrypt the file and handle exceptions
                try:
                    #encode(content,password,version='python-ecdsa-0.1',format=None)
                    #result = cls.encencrypt_file(src_file, key, dest_file)
                    result = UtilCrypto.encrypt_file(src_file, key, dest_file)
                    if not result:
                        print(f"Failed to encrypt {src_file}")
                except Exception as e:
                    print(f"Error encrypting {src_file}: {e}")

        # Save the file hashes to .enc.info
        try:
            os.makedirs(dest_dir, exist_ok=True)
            with open(enc_info_file, 'w') as f:
                json.dump(file_hashes, f)
            print(f"Encryption info saved to {enc_info_file}")
        except Exception as e:
            print(f"Error saving encryption info: {e}")


    @staticmethod
    def get_file_cid(file_path, ipfs_path="ipfs" ):
        print("WAT")
        return crypto.get_file_cid(file_path, ipfs_path)

    @staticmethod
    def decrypt_file(src_file: str, key: dict, dest_file: str) -> bool:
        """
        Decrypts a single file using the crypto.decode method.
        Decrypts the content, then decodes the base64 string back to binary data.

        :param src_file: The path to the encrypted source file.
        :param key_info: A dictionary containing key information for decryption.
        :param dest_file: The path where the decrypted file will be saved.
        :return: True if decryption was successful, False otherwise.
        """
        try:
            # Read the encrypted content from the source file in text mode
            with open(src_file, 'r', encoding='utf-8') as f:
                encrypted_content = f.read()

            # Get the decryption password from key_info
            password = key

            # Decrypt the content using crypto.decode
            decrypted_base64 = crypto.decode(encrypted_content, password)

            # Convert the base64 string back to binary data
            content = base64.b64decode(decrypted_base64.encode('utf-8'))

            # Write the binary content to the destination file in binary mode
            with open(dest_file, 'wb') as f:
                f.write(content)

            return True
        except Exception as e:
            print(f"Error decrypting file {src_file}: {e}")
            return False

    @staticmethod
    def decrypt_directory(source_dir: str, dest_dir: str, key: dict):
        """
        Decrypt files listed in .enc.info, verify their hashes, and raise an exception if verification fails.

        :param source_dir: The path to the source directory containing encrypted files.
        :param dest_dir: The path to the destination directory where decrypted files will be saved.
        :param key: A dictionary containing key information for decryption.
        """
        enc_info_file = os.path.join(source_dir, '.enc.info')
        
        # Read the .enc.info file
        with open(enc_info_file, 'r') as f:
            file_hashes = json.load(f)


        for rel_path, expected_hash in file_hashes.items():
            # Construct the source and destination file paths
            src_file = os.path.join(source_dir, rel_path)
            dest_file = os.path.join(dest_dir, rel_path)

            # Ensure the destination directory exists
            dest_dir_path = os.path.dirname(dest_file)
            os.makedirs(dest_dir_path, exist_ok=True)

            # Decrypt the file and handle exceptions
            try:
                result = UtilCrypto.decrypt_file(src_file, key, dest_file)
                if not result:
                    print(f"Failed to decrypt {src_file}")
                    continue
            except Exception as e:
                print(f"Error decrypting {src_file}: {e}")
                continue

            # Compute the hash of the decrypted file and verify it
            try:
                actual_hash = UtilCrypto.compute_file_hash(dest_file)
                if actual_hash != expected_hash:
                    raise Exception(f"Hash mismatch for {dest_file}: expected {expected_hash}, got {actual_hash}")
            except Exception as e:
                print(f"Error verifying hash for {dest_file}: {e}")
                raise

        print("Decryption and verification completed successfully.")

if __name__ == "__main__":
    UtilCrypto.run_cli()