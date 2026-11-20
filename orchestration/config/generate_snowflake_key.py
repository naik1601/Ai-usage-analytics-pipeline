from getpass import getpass
from pathlib import Path

from cryptography.hazmat.primitives import serialization
from cryptography.hazmat.primitives.asymmetric import rsa


folder = Path("/opt/airflow/config")
private_path = folder / "snowflake_key.p8"
public_path = folder / "snowflake_key.pub"

if private_path.exists() or public_path.exists():
    raise SystemExit("Key files already exist; refusing to overwrite.")

passphrase = getpass("Choose a private-key passphrase: ")
confirmation = getpass("Confirm passphrase: ")

if not passphrase or passphrase != confirmation:
    raise SystemExit("Passphrases must match and cannot be empty.")

key = rsa.generate_private_key(
    public_exponent=65537,
    key_size=2048,
)

private_path.write_bytes(
    key.private_bytes(
        encoding=serialization.Encoding.PEM,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.BestAvailableEncryption(
            passphrase.encode("utf-8")
        ),
    )
)
private_path.chmod(0o600)

public_pem = key.public_key().public_bytes(
    encoding=serialization.Encoding.PEM,
    format=serialization.PublicFormat.SubjectPublicKeyInfo,
).decode("utf-8")

public_path.write_text(public_pem, encoding="utf-8")

public_value = "".join(
    line for line in public_pem.splitlines()
    if not line.startswith("-----")
)

print("\nPublic key for Snowflake:\n")
print(public_value)