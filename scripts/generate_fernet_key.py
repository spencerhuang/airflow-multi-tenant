#!/usr/bin/env python3
"""Generate a Fernet key for Airflow and write it to docker/secrets/fernet_key."""

import os
import sys

from cryptography.fernet import Fernet


def main():
    project_root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    secrets_dir = os.path.join(project_root, "docker", "secrets")
    os.makedirs(secrets_dir, exist_ok=True)

    fernet_path = os.path.join(secrets_dir, "fernet_key")

    if os.path.exists(fernet_path):
        print(f"Fernet key already exists at {fernet_path}")
        print("Delete it manually if you want to regenerate.")
        sys.exit(0)

    key = Fernet.generate_key().decode()
    with open(fernet_path, "w") as f:
        f.write(key)

    print(f"Fernet key generated: {fernet_path}")


if __name__ == "__main__":
    main()
