import json
import pandas as pd
from web3 import Web3, HTTPProvider
from web3.eth import Eth
from typing import List, Dict, Any, Optional
from requests import Session
import config
from logger import get_logger

def load_wallets(excel_path: str) -> List[Dict[str, Any]]:
    """Loads wallets from Excel file."""
    df = pd.read_excel(excel_path)
    df.columns = df.columns.str.lower().str.strip()
    required_columns = {"private_key", "cex_deposit_address"}
    if config.USE_PROXY:
        required_columns.add("proxy")

    if not required_columns.issubset(set(df.columns)):
        raise ValueError(f"Excel must contain columns: {required_columns}")

    for pk in df["private_key"]:
        if not isinstance(pk, str) or len(pk) != 66 or not pk.startswith("0x"):
            raise ValueError(f"Invalid private_key: {pk}")

    if "proxy" in df.columns:
        df["proxy"] = df["proxy"].where(pd.notnull(df["proxy"]), None)

    return df.to_dict(orient="records")

def load_abi(path: str) -> Any:
    """Loads contract ABI from JSON file."""
    with open(path) as f:
        return json.load(f)

def get_w3(rpc_url: str, proxy: Optional[str] = None) -> Web3:
    """Returns Web3 connection to RPC."""
    if proxy:
        session = Session()
        session.proxies = {'http': proxy, 'https': proxy}
        provider = HTTPProvider(rpc_url, request_kwargs={'timeout': 30}, session=session)
    else:
        provider = HTTPProvider(rpc_url, request_kwargs={'timeout': 30})
    return Web3(provider)

def get_w3_with_retry(rpc_url: str, proxy: Optional[str] = None) -> Optional[Web3]:
    """Returns Web3 connection with retries."""
    logger = get_logger("Utils", config.LOG_LEVEL)
    for attempt in range(1, config.RPC_TRY + 1):
        try:
            w3 = get_w3(rpc_url, proxy)
            if w3.eth.chain_id:
                return w3
        except Exception as e:
            logger.warning(f"Attempt {attempt}/{config.RPC_TRY} failed for {rpc_url}: {e}")
            if attempt == config.RPC_TRY:
                logger.error(f"All attempts failed for {rpc_url}")
                return None
    return None