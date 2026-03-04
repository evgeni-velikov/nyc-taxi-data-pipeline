import os
from dotenv import load_dotenv


load_dotenv()


def require_env(name: str, default: str | None = None) -> str:
    value = os.getenv(name, default)
    if value is None:
        raise RuntimeError(f"Missing required env variable: {name}")
    return value
