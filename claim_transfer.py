# claim_transfer.py
import asyncio
import time
import csv
import threading
from concurrent.futures import ThreadPoolExecutor
from typing import Dict, Any, Callable, Optional

from eth_account import Account

import config
from utils import load_wallets, load_abi, get_w3_with_retry
from logger import get_logger

from web3 import Web3
from web3.eth import Eth

logger = get_logger("ClaimTransfer", config.LOG_LEVEL)

# Lock for file writes to prevent concurrent writes
_file_write_lock = threading.Lock()


def shorten_address(address: str) -> str:
    return f"{address[:6]}...{address[-4:]}"


def validate_private_key(private_key: str) -> None:
    if not isinstance(private_key, str):
        raise ValueError("Private key must be a string")
    if not private_key.startswith("0x") or len(private_key) != 66:
        raise ValueError("Invalid private key format: must be 0x + 64 hex chars")


def _append_failed(wallet_address: str, deposit_address: str, reason: str, proxy: Optional[str]) -> None:
    with _file_write_lock:
        try:
            with open(config.FAILED_WALLETS_FILE, "a", newline="") as f:
                f.write(f"{wallet_address},{deposit_address},{reason},{proxy or ''}\n")
        except Exception as e:
            logger.error(f"Failed to write to {config.FAILED_WALLETS_FILE}: {e}")


def _mark_processed(wallet_address: str, deposit_address: str) -> None:
    # Append processed record (wallet, deposit, timestamp)
    with _file_write_lock:
        try:
            with open(config.PROCESSED_FILE, "a", newline="") as f:
                writer = csv.writer(f)
                writer.writerow([wallet_address.lower(), deposit_address, int(time.time())])
        except Exception as e:
            logger.error(f"Failed to write to {config.PROCESSED_FILE}: {e}")


def _is_processed(wallet_address: str) -> bool:
    try:
        address_l = wallet_address.lower()
        with open(config.PROCESSED_FILE, "r", newline="") as f:
            for line in f:
                if not line:
                    continue
                parts = line.strip().split(",")
                if not parts:
                    continue
                if parts[0].lower() == address_l:
                    return True
    except FileNotFoundError:
        return False
    except Exception as e:
        logger.warning(f"Failed to read processed file: {e}")
    return False


def get_token_balance(address: str, proxy: Optional[str]) -> int:
    token_abi = load_abi(config.ERC20_ABI_PATH)
    for rpc in config.RPC_LIST:
        w3 = get_w3_with_retry(rpc, proxy)
        if w3 is None:
            continue
        try:
            token_contract = w3.eth.contract(address=config.LINEA_TOKEN_ADDRESS, abi=token_abi)
            balance = token_contract.functions.balanceOf(address).call()
            return int(balance)
        except Exception as e:
            logger.warning(f"Balance check failed on {rpc}: {e}")
    logger.error(f"Failed to fetch token balance for {shorten_address(address)} on all RPCs")
    return -1


def check_allocation(wallet: Any, proxy: Optional[str], wallet_index: int) -> int:
    claim_abi = load_abi(config.CLAIM_CONTRACT_ABI_PATH)
    for rpc in config.RPC_LIST:
        w3 = get_w3_with_retry(rpc, proxy)
        if w3 is None:
            continue
        try:
            claim_contract = w3.eth.contract(address=config.CLAIM_CONTRACT_ADDRESS, abi=claim_abi)
            allocation = claim_contract.functions.calculateAllocation(wallet.address).call()
            logger.info(
                f"Wallet #{wallet_index} ({shorten_address(wallet.address)}): Allocation = {allocation / 1e18:.6f} $LINEA"
            )
            return int(allocation)
        except Exception as e:
            logger.warning(f"Allocation check failed on {rpc}: {e}")
    logger.error(f"Failed to check allocation for {shorten_address(wallet.address)}")
    return -1


async def wait_for_contract_balance(proxy: Optional[str], wallet_index: int, wallet_short: str) -> bool:
    """Асинхронно ждём пока контракт получит токены"""
    while True:
        balance = get_token_balance(config.CLAIM_CONTRACT_ADDRESS, proxy)
        if balance == -1:
            logger.error(f"Wallet #{wallet_index} ({wallet_short}): Failed to check contract balance")
            return False
        if balance > 0:
            logger.info(
                f"Wallet #{wallet_index} ({wallet_short}): Contract has {balance / 1e18:.6f} $LINEA, proceeding with claim"
            )
            return True
        logger.warning(
            f"Wallet #{wallet_index} ({wallet_short}): Contract balance is 0, waiting {config.CHECK_INTERVAL} seconds..."
        )
        await asyncio.sleep(config.CHECK_INTERVAL)


def _compute_eip1559_fees(w3: Web3) -> Dict[str, int]:
    """Compute EIP-1559 fields: returns dict with maxPriorityFeePerGas and maxFeePerGas (ints)."""
    # priority fee
    try:
        priority = w3.eth.max_priority_fee
        if priority is None:
            raise Exception("max_priority_fee not available")
    except Exception:
        # fallback to small fraction of gas_price
        try:
            priority = int(w3.eth.gas_price * 0.1)
        except Exception:
            priority = 1_000_000_000  # 1 gwei fallback

    # base fee from pending block
    try:
        pending = w3.eth.get_block("pending")
        base_fee = pending.get("baseFeePerGas", None)
        if base_fee is None:
            base_fee = w3.eth.gas_price
    except Exception:
        base_fee = w3.eth.gas_price

    max_fee = int((int(base_fee) * 2 + int(priority)) * config.GAS_PRICE_MULTIPLIER)
    return {"maxPriorityFeePerGas": int(priority), "maxFeePerGas": int(max_fee)}


def sync_send_transaction(
    wallet: Any,
    txn_builder: Callable[[Web3, int, Dict[str, int], int], Dict[str, Any]],
    proxy: Optional[str],
    timeout: int = config.TX_TIMEOUT,
) -> tuple[bool, Optional[str]]:
    """Send transaction with RPC retries. Uses EIP-1559 fee fields."""
    for rpc in config.RPC_LIST:
        w3 = get_w3_with_retry(rpc, proxy)
        if w3 is None:
            logger.debug(f"RPC {rpc} unreachable, skipping")
            continue
        eth = Eth(w3)
        for attempt in range(1, config.RPC_TRY + 1):
            try:
                eth_balance = eth.get_balance(wallet.address)
                logger.debug(f"Wallet {shorten_address(wallet.address)}: ETH balance = {eth_balance / 1e18:.6f} ETH")

                nonce = eth.get_transaction_count(wallet.address)
                logger.debug(f"Wallet {shorten_address(wallet.address)}: Nonce = {nonce}, attempt {attempt}/{config.RPC_TRY}")

                fee_params = _compute_eip1559_fees(w3)

                # Build a temp tx for gas estimation
                temp_txn = txn_builder(w3, nonce, fee_params, 1_000_000)
                try:
                    gas_limit = eth.estimate_gas(temp_txn)
                    gas_limit = int(gas_limit * 1.2)
                    logger.debug(f"Estimated gas: {gas_limit}")
                except Exception as e:
                    logger.warning(f"Gas estimation failed (attempt {attempt}) on {rpc}: {e}")
                    # If revert occurs it's likely the call would revert -> don't spam retries
                    if "execution reverted" in str(e).lower():
                        logger.error(f"Contract execution reverted during gas estimate: {e}")
                        return False, None
                    continue

                txn = txn_builder(w3, nonce, fee_params, gas_limit)
                txn["chainId"] = w3.eth.chain_id

                # Remove gasPrice if present to ensure EIP-1559 tx
                txn.pop("gasPrice", None)
                txn["maxPriorityFeePerGas"] = fee_params["maxPriorityFeePerGas"]
                txn["maxFeePerGas"] = fee_params["maxFeePerGas"]

                try:
                    signed_txn = w3.eth.account.sign_transaction(txn, wallet.key)
                except Exception as e:
                    logger.warning(f"Signing failed on attempt {attempt} for {shorten_address(wallet.address)}: {e}")
                    continue

                try:
                    tx_hash = eth.send_raw_transaction(signed_txn.raw_transaction)
                    logger.info(f"Transaction sent ({shorten_address(wallet.address)}) [{rpc}], tx: {tx_hash.hex()}")
                except Exception as e:
                    logger.warning(f"Send failed (attempt {attempt}) on {rpc}: {e}")
                    continue

                try:
                    receipt = eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
                    if receipt.status == 1:
                        logger.info(f"Transaction successful ({shorten_address(wallet.address)}) [{rpc}], tx: {tx_hash.hex()}")
                        return True, tx_hash.hex()
                    else:
                        logger.warning(f"Transaction reverted ({shorten_address(wallet.address)}) [{rpc}], tx: {tx_hash.hex()}")
                        return False, tx_hash.hex()
                except Exception as e:
                    logger.warning(f"Receipt wait failed (attempt {attempt}) on {rpc}: {e}")
                    continue

            except Exception as e:
                logger.warning(f"General error on RPC {rpc} attempt {attempt}: {e}")
                continue

        logger.error(f"RPC {rpc} exhausted attempts, switching to next RPC if available.")
    return False, None


def check_has_claimed(wallet: Any, proxy: Optional[str], wallet_index: int) -> Optional[bool]:
    claim_abi = load_abi(config.CLAIM_CONTRACT_ABI_PATH)
    for rpc in config.RPC_LIST:
        w3 = get_w3_with_retry(rpc, proxy)
        if w3 is None:
            continue
        try:
            claim_contract = w3.eth.contract(address=config.CLAIM_CONTRACT_ADDRESS, abi=claim_abi)
            has_claimed = claim_contract.functions.hasClaimed(wallet.address).call()
            logger.info(f"Wallet #{wallet_index} ({shorten_address(wallet.address)}): hasClaimed = {has_claimed}")
            return bool(has_claimed)
        except Exception as e:
            logger.warning(f"hasClaimed check failed on {rpc}: {e}")
    logger.error(f"Failed hasClaimed check for {shorten_address(wallet.address)} on all RPCs")
    return None


def sync_claim_only(wallet: Any, proxy: Optional[str], wallet_index: int) -> bool:
    claim_abi = load_abi(config.CLAIM_CONTRACT_ABI_PATH)
    wallet_short = shorten_address(wallet.address)

    if _is_processed(wallet.address):
        logger.info(f"Wallet #{wallet_index} ({wallet_short}): already processed, skipping")
        return True

    has_claimed = check_has_claimed(wallet, proxy, wallet_index)
    if has_claimed is None:
        logger.error(f"Wallet #{wallet_index} ({wallet_short}): hasClaimed check failed")
        _append_failed(wallet.address, "", "hasClaimed_check_failed", proxy)
        return False
    if has_claimed:
        logger.info(f"Wallet #{wallet_index} ({wallet_short}): already claimed, marking processed")
        _mark_processed(wallet.address, "")
        return True

    allocation = check_allocation(wallet, proxy, wallet_index)
    if allocation <= 0:
        logger.warning(f"Wallet #{wallet_index} ({wallet_short}): No allocation, skipping")
        _append_failed(wallet.address, "", "no_allocation", proxy)
        return False

    # Wait for contract to have tokens (async)
    loop = asyncio.new_event_loop()
    try:
        ok = loop.run_until_complete(wait_for_contract_balance(proxy, wallet_index, wallet_short))
    finally:
        loop.close()
    if not ok:
        _append_failed(wallet.address, "", "contract_empty", proxy)
        return False

    # Attempt claim with retries (loop, not recursion)
    for attempt in range(1, config.WALLET_RETRIES + 1):
        logger.info(f"Wallet #{wallet_index} ({wallet_short}): Claim attempt {attempt}/{config.WALLET_RETRIES}")

        def build_claim_tx(w3: Web3, nonce: int, fee_params: Dict[str, int], gas_limit: int) -> Dict[str, Any]:
            contract = w3.eth.contract(address=config.CLAIM_CONTRACT_ADDRESS, abi=claim_abi)
            tx = contract.functions.claim().build_transaction({
                "from": wallet.address,
                "nonce": nonce,
                "gas": gas_limit,
            })
            return tx

        success, tx_hash = sync_send_transaction(wallet, build_claim_tx, proxy)
        if success:
            logger.info(f"Wallet #{wallet_index} ({wallet_short}): Claim successful, tx {tx_hash}")
            _mark_processed(wallet.address, "")
            return True
        else:
            logger.warning(f"Wallet #{wallet_index} ({wallet_short}): Claim failed on attempt {attempt}")
            if attempt < config.WALLET_RETRIES:
                time.sleep(config.RETRY_DELAY_SEC)

    _append_failed(wallet.address, "", "claim_failed", proxy)
    return False


def sync_claim_and_transfer(wallet: Any, deposit_address: str, proxy: Optional[str], wallet_index: int) -> bool:
    claim_abi = load_abi(config.CLAIM_CONTRACT_ABI_PATH)
    token_abi = load_abi(config.ERC20_ABI_PATH)
    wallet_short = shorten_address(wallet.address)

    if _is_processed(wallet.address):
        logger.info(f"Wallet #{wallet_index} ({wallet_short}): already processed, skipping")
        return True

    has_claimed = check_has_claimed(wallet, proxy, wallet_index)
    if has_claimed is None:
        logger.error(f"Wallet #{wallet_index} ({wallet_short}): hasClaimed check failed")
        _append_failed(wallet.address, deposit_address, "hasClaimed_check_failed", proxy)
        return False
    if has_claimed:
        logger.info(f"Wallet #{wallet_index} ({wallet_short}): already claimed, marking processed")
        _mark_processed(wallet.address, deposit_address)
        return True

    allocation = check_allocation(wallet, proxy, wallet_index)
    if allocation <= 0:
        logger.warning(f"Wallet #{wallet_index} ({wallet_short}): No allocation, skipping")
        _append_failed(wallet.address, deposit_address, "no_allocation", proxy)
        return False

    # Wait for contract to have tokens
    loop = asyncio.new_event_loop()
    try:
        ok = loop.run_until_complete(wait_for_contract_balance(proxy, wallet_index, wallet_short))
    finally:
        loop.close()
    if not ok:
        _append_failed(wallet.address, deposit_address, "contract_empty", proxy)
        return False

    # Claim with retries
    claim_tx_hash = None
    for attempt in range(1, config.WALLET_RETRIES + 1):
        logger.info(f"Wallet #{wallet_index} ({wallet_short}): Claim attempt {attempt}/{config.WALLET_RETRIES}")

        def build_claim_tx(w3: Web3, nonce: int, fee_params: Dict[str, int], gas_limit: int) -> Dict[str, Any]:
            contract = w3.eth.contract(address=config.CLAIM_CONTRACT_ADDRESS, abi=claim_abi)
            tx = contract.functions.claim().build_transaction({
                "from": wallet.address,
                "nonce": nonce,
                "gas": gas_limit,
            })
            return tx

        claim_success, claim_tx_hash = sync_send_transaction(wallet, build_claim_tx, proxy)
        if claim_success:
            logger.info(f"Wallet #{wallet_index} ({wallet_short}): Claim successful, tx {claim_tx_hash}")
            break
        else:
            logger.warning(f"Wallet #{wallet_index} ({wallet_short}): Claim failed on attempt {attempt}")
            if attempt < config.WALLET_RETRIES:
                time.sleep(config.RETRY_DELAY_SEC)
    else:
        _append_failed(wallet.address, deposit_address, "claim_failed", proxy)
        return False

    # After successful claim, read token balance
    balance = get_token_balance(wallet.address, proxy)
    if balance <= 0:
        logger.warning(f"Wallet #{wallet_index} ({wallet_short}): Claimed 0 $LINEA")
        _append_failed(wallet.address, deposit_address, "zero_balance_after_claim", proxy)
        return False

    # Transfer with retries
    for attempt in range(1, config.WALLET_RETRIES + 1):
        logger.info(f"Wallet #{wallet_index} ({wallet_short}): Transfer attempt {attempt}/{config.WALLET_RETRIES}")

        def build_transfer_tx(w3: Web3, nonce: int, fee_params: Dict[str, int], gas_limit: int) -> Dict[str, Any]:
            token_contract = w3.eth.contract(address=config.LINEA_TOKEN_ADDRESS, abi=token_abi)
            tx = token_contract.functions.transfer(deposit_address, balance).build_transaction({
                "from": wallet.address,
                "nonce": nonce,
                "gas": gas_limit,
            })
            return tx

        transfer_success, transfer_tx_hash = sync_send_transaction(wallet, build_transfer_tx, proxy)
        if transfer_success:
            logger.info(
                f"Wallet #{wallet_index} ({wallet_short}): Transfer completed, {balance} $LINEA, tx: {transfer_tx_hash}"
            )
            _mark_processed(wallet.address, deposit_address)
            return True
        else:
            logger.warning(f"Wallet #{wallet_index} ({wallet_short}): Transfer failed on attempt {attempt}")
            if attempt < config.WALLET_RETRIES:
                time.sleep(config.RETRY_DELAY_SEC)

    _append_failed(wallet.address, deposit_address, "transfer_failed", proxy)
    return False


async def _claim_only_async(wallet: Any, proxy: Optional[str], wallet_index: int, executor: ThreadPoolExecutor) -> bool:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, sync_claim_only, wallet, proxy, wallet_index)


async def _claim_and_transfer_async(wallet: Any, deposit_address: str, proxy: Optional[str], wallet_index: int, executor: ThreadPoolExecutor) -> bool:
    loop = asyncio.get_running_loop()
    return await loop.run_in_executor(executor, sync_claim_and_transfer, wallet, deposit_address, proxy, wallet_index)


async def worker(wallet_data: Dict[str, Any], index: int, claim_only_mode: bool, executor: ThreadPoolExecutor) -> None:
    try:
        private_key = wallet_data["private_key"]
        validate_private_key(private_key)
        wallet = Account.from_key(private_key)
    except Exception as e:
        logger.error(f"Wallet #{index}: Invalid private key: {e}")
        return

    deposit_address = wallet_data["cex_deposit_address"]
    proxy = wallet_data.get("proxy") if config.USE_PROXY else None
    wallet_short = shorten_address(wallet.address)

    if config.USE_PROXY and not proxy:
        logger.error(f"Wallet #{index} ({wallet_short}): Skipped, proxy required")
        return

    if claim_only_mode:
        await _claim_only_async(wallet, proxy, index, executor)
    else:
        await _claim_and_transfer_async(wallet, deposit_address, proxy, index, executor)


async def run_claim_workers(claim_only_mode: bool = False) -> None:
    wallets = load_wallets(config.EXCEL_PATH)
    logger.info(f"Loaded {len(wallets)} wallets for {'claim only' if claim_only_mode else 'claim and transfer'}")

    # ThreadPoolExecutor to limit blocking sync tasks
    executor = ThreadPoolExecutor(max_workers=max(1, config.WALLETS_IN_WORK))

    try:
        batch_size = config.WALLETS_IN_WORK
        for batch_start in range(0, len(wallets), batch_size):
            batch = wallets[batch_start:batch_start + batch_size]
            logger.info(
                f"Processing batch {batch_start // batch_size + 1} (wallets #{batch_start + 1} to #{min(batch_start + batch_size, len(wallets))})"
            )

            tasks = []
            for i, wallet_data in enumerate(batch):
                # Validate per-wallet; skip invalid ones
                try:
                    validate_private_key(wallet_data["private_key"])
                except Exception as e:
                    logger.error(f"Wallet #{batch_start + i + 1}: Invalid private key: {e}")
                    continue

                delay = i * config.SLEEP_BETWEEN_WALLETS_SEC

                async def scheduled_task(wd, idx, d):
                    await asyncio.sleep(d)
                    await worker(wd, idx, claim_only_mode, executor)

                t = asyncio.create_task(scheduled_task(wallet_data, batch_start + i + 1, delay))
                tasks.append(t)

            if tasks:
                await asyncio.gather(*tasks)
            logger.info(f"Batch {batch_start // batch_size + 1} completed")
    finally:
        executor.shutdown(wait=True)