import os


# TODO upgrade to support dotenv, env file yml file configs, and os env vars
def require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value:
        raise RuntimeError(f"Missing required env variable: {name}")
    return value
