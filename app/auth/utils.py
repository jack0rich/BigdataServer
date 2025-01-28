from bcrypt import hashpw, gensalt, checkpw
from cryptography.fernet import Fernet
import os
from dotenv import load_dotenv
import secrets
import string


load_dotenv(dotenv_path='../../pg-docker/.env')
ENCRYPTION_KEY = os.getenv("ENCRYPTION_KEY")
cipher_suite = Fernet(ENCRYPTION_KEY)


def hash_password(password: str) -> str:
    return hashpw(password.encode(), gensalt(rounds=12)).decode()


def verify_password(plain_password: str, hashed_password: str) -> bool:
    return checkpw(plain_password.encode(), hashed_password.encode())


def encrypt_key(api_key: str) -> bytes:
    return cipher_suite.encrypt(api_key.encode())


def decrypt_key(encrypted_key: bytes) -> str:
    return cipher_suite.decrypt(encrypted_key).decode()


def generate_secure_api_key() -> str:
    # Step 1: Generate a random 32-character string
    api_key = ''.join(secrets.choice(string.ascii_letters + string.digits) for _ in range(64))

    # Encrypt the API key using Fernet
    encrypted_key = cipher_suite.encrypt(api_key.encode("utf-8")).decode("utf-8")

    # Format the encrypted key with a prefix
    formatted_key = f"sk_{encrypted_key[:32]}_{encrypted_key[32:64]}"

    return formatted_key


if __name__ == '__main__':
    print(generate_secure_api_key())

