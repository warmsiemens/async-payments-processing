import secrets


def api_key_matches(key: str | None, expected: str) -> bool:
    return key is not None and secrets.compare_digest(key.encode(), expected.encode())
