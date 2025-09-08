# utils.py
import json
import pandas as pd
from web3 import Web3, HTTPProvider
from web3.eth import Eth
from typing import List, Dict, Any, Optional
from requests import Session
import config
from logger import get_logger

logger = get_logger("Utils", config.LOG_LEVEL)

def load_wallets(excel_path: str) -> List[Dict[str, Any]]:
    """Loads wallets from Excel file. Validates row-by-row and returns only valid entries.
    Expected columns: private_key, cex_deposit_address, optional proxy.
    """
    df = pd.read_excel(excel_path, engine="openpyxl")
    df.columns = df.columns.str.lower().str.strip()
    required_columns = {"private_key", "cex_deposit_address"}
    if config.USE_PROXY:
        required_columns.add("proxy")

    if not required_columns.issubset(set(df.columns)):
        raise ValueError(f"Excel must contain columns: {required_columns}")

    # Normalize proxies column
    if "proxy" in df.columns:
        df["proxy"] = df["proxy"].where(pd.notnull(df["proxy"]), None)

    valid_rows = []
    for idx, row in df.iterrows():
        pk = row["private_key"]
        deposit = row["cex_deposit_address"]
        proxy = row.get("proxy", None) if "proxy" in df.columns else None

        # Ensure pk is string
        if isinstance(pk, float) or isinstance(pk, int):
            pk = str(pk)
        if not isinstance(pk, str):
            logger.error(f"Row {idx+2}: private_key is not a string, skipping")
            continue
        pk = pk.strip()
        # allow keys both '0x...' and without 0x. Normalize:
        if not pk.startswith("0x"):
            pk = "0x" + pk
        if len(pk) != 66:
            logger.error(f"Row {idx+2}: Invalid private_key length ({pk}), skipping")
            continue
        if not all(c in "0123456789abcdefABCDEF" for c in pk[2:]):
            logger.error(f"Row {idx+2}: private_key contains non-hex characters, skipping")
            continue

        # deposit address minimal validation
        if not isinstance(deposit, str) or not deposit.startswith("0x") or len(deposit) != 42:
            logger.error(f"Row {idx+2}: invalid cex_deposit_address ({deposit}), skipping")
            continue

        valid_rows.append({
            "private_key": pk,
            "cex_deposit_address": deposit,
            "proxy": proxy
        })

    logger.info(f"Loaded {len(valid_rows)} valid wallets from {excel_path}")
    return valid_rows


def load_abi(path: str) -> Any:
    """Loads contract ABI from JSON file."""
    with open(path) as f:
        return json.load(f)


def get_w3(rpc_url: str, proxy: Optional[str] = None) -> Web3:
    """Returns Web3 connection to RPC (with optional HTTP proxy session)."""
    if proxy:
        session = Session()
        session.proxies = {'http': proxy, 'https': proxy}
        provider = HTTPProvider(rpc_url, request_kwargs={'timeout': 30}, session=session)
    else:
        provider = HTTPProvider(rpc_url, request_kwargs={'timeout': 30})
    return Web3(provider)


def get_w3_with_retry(rpc_url: str, proxy: Optional[str] = None) -> Optional[Web3]:
    """Returns Web3 connection with retries and verifies chain_id matches config.CHAIN_ID."""
    for attempt in range(1, config.RPC_TRY + 1):
        try:
            w3 = get_w3(rpc_url, proxy)
            chain_id = w3.eth.chain_id
            if chain_id == config.CHAIN_ID:
                return w3
            else:
                logger.warning(f"{rpc_url}: unexpected chain_id {chain_id} (expected {config.CHAIN_ID})")
                return None
        except Exception as e:
            logger.warning(f"Attempt {attempt}/{config.RPC_TRY} failed for {rpc_url}: {e}")
    logger.error(f"All attempts failed for {rpc_url}")
    return None
