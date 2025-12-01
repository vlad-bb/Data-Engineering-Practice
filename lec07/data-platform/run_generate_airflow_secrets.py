"""
Install required package:
    pip install cryptography
"""

import os

from cryptography.fernet import Fernet


def generate_fernet_key():
    """
    Generate a new Fernet key for Airflow encryption.

    Returns:
        str: A newly generated Fernet key as a string.
    """
    return Fernet.generate_key().decode()


def generate_secret_key(length=32):
    """
    Generate a random secret key for Airflow.

    Args:
        length (int): Length of the secret key. Default is 32.

    Returns:
        str: A randomly generated secret key.
    """
    return os.urandom(length).hex()


def generate_airflow_secrets():
    """
    Generate and print Airflow secrets: Fernet key and secret key.
    """
    fernet_key = generate_fernet_key()
    secret_key = generate_secret_key()

    print("Fernet Key:", fernet_key)
    print("Secret Key:", secret_key)


if __name__ == "__main__":
    generate_airflow_secrets()
