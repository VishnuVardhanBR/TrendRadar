# coding=utf-8
"""
Configuration Utilities - Multi-account config parsing and validation

Provides parsing, validation, and limiting for multi-account push configurations
"""

from typing import Dict, List, Optional, Tuple


def parse_multi_account_config(config_value: str, separator: str = ";") -> List[str]:
    """
    Parse multi-account config, return account list

    Args:
        config_value: Config string, accounts separated by delimiter
        separator: Delimiter, default is ;

    Returns:
        Account list, empty strings are preserved (for placeholders)

    Examples:
        >>> parse_multi_account_config("url1;url2;url3")
        ['url1', 'url2', 'url3']
        >>> parse_multi_account_config(";token2")  # First account has no token
        ['', 'token2']
        >>> parse_multi_account_config("")
        []
    """
    if not config_value:
        return []
    # Preserve empty strings for placeholders (e.g., ";token2" means first account has no token)
    accounts = [acc.strip() for acc in config_value.split(separator)]
    # Filter out if all are empty
    if all(not acc for acc in accounts):
        return []
    return accounts


def validate_paired_configs(
    configs: Dict[str, List[str]],
    channel_name: str,
    required_keys: Optional[List[str]] = None
) -> Tuple[bool, int]:
    """
    Validate that paired configs have matching counts

    For channels requiring multiple paired configs (e.g., Telegram token and chat_id),
    validates that all config items have the same number of accounts.

    Args:
        configs: Config dictionary, key is config name, value is account list
        channel_name: Channel name for logging
        required_keys: List of required config items

    Returns:
        (validation passed, account count)

    Examples:
        >>> validate_paired_configs({
        ...     "token": ["t1", "t2"],
        ...     "chat_id": ["c1", "c2"]
        ... }, "Telegram", ["token", "chat_id"])
        (True, 2)

        >>> validate_paired_configs({
        ...     "token": ["t1", "t2"],
        ...     "chat_id": ["c1"]  # Count mismatch
        ... }, "Telegram", ["token", "chat_id"])
        (False, 0)
    """
    # Filter out empty lists
    non_empty_configs = {k: v for k, v in configs.items() if v}

    if not non_empty_configs:
        return True, 0

    # Check required items
    if required_keys:
        for key in required_keys:
            if key not in non_empty_configs or not non_empty_configs[key]:
                return True, 0  # Required item is empty, treat as not configured

    # Get lengths of all non-empty configs
    lengths = {k: len(v) for k, v in non_empty_configs.items()}
    unique_lengths = set(lengths.values())

    if len(unique_lengths) > 1:
        print(f"❌ {channel_name} config error: Paired config counts don't match, skipping this channel")
        for key, length in lengths.items():
            print(f"   - {key}: {length} items")
        return False, 0

    return True, list(unique_lengths)[0] if unique_lengths else 0


def limit_accounts(
    accounts: List[str],
    max_count: int,
    channel_name: str
) -> List[str]:
    """
    Limit account count

    When configured accounts exceed max limit, only use first N accounts
    and output warning.

    Args:
        accounts: Account list
        max_count: Max account count
        channel_name: Channel name for logging

    Returns:
        Limited account list

    Examples:
        >>> limit_accounts(["a1", "a2", "a3"], 2, "Slack")
        ⚠️ Slack has 3 accounts configured, exceeds max limit 2, using first 2 only
        ['a1', 'a2']
    """
    if len(accounts) > max_count:
        print(f"⚠️ {channel_name} has {len(accounts)} accounts configured, exceeds max limit {max_count}, using first {max_count} only")
        print(f"   ⚠️ Warning: If you're a fork user, too many accounts may cause long GitHub Actions run times")
        return accounts[:max_count]
    return accounts


def get_account_at_index(accounts: List[str], index: int, default: str = "") -> str:
    """
    Safely get account value at specified index

    Returns default when index is out of range or account value is empty.

    Args:
        accounts: Account list
        index: Index
        default: Default value

    Returns:
        Account value or default

    Examples:
        >>> get_account_at_index(["a", "b", "c"], 1)
        'b'
        >>> get_account_at_index(["a", "", "c"], 1, "default")
        'default'
        >>> get_account_at_index(["a"], 5, "default")
        'default'
    """
    if index < len(accounts):
        return accounts[index] if accounts[index] else default
    return default
