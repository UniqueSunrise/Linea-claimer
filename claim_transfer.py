import asyncio
import time
from eth_account import Account
from typing import Dict, Any, Callable, Optional

import config
from utils import load_wallets, load_abi, get_w3_with_retry
from logger import get_logger

from web3 import Web3
from web3.eth import Eth

logger = get_logger("ClaimTransfer", config.LOG_LEVEL)



def shorten_address(address: str) -> str:
    """Shortens an Ethereum address for logging."""
    return f"{address[:6]}...{address[-4:]}"


def validate_private_key(private_key: str) -> None:
    """Validates private key format."""
    if not private_key.startswith("0x") or len(private_key) != 66:
        raise ValueError(f"Invalid private key format: must be 64 hex chars with 0x prefix, got {private_key[:10]}...")


def get_token_balance(address: str, proxy: Optional[str]) -> int:
    """Fetches $LINEA token balance with RPC retries."""
    token_abi = load_abi(config.ERC20_ABI_PATH)
    for rpc in config.RPC_LIST:
        w3 = get_w3_with_retry(rpc, proxy)
        if w3 is None:
            logger.warning(f"Failed to connect to RPC {rpc} for balance check")
            continue
        try:
            chain_id = w3.eth.chain_id
            logger.debug(f"Connected to RPC {rpc}, chain_id: {chain_id}")
            token_contract = w3.eth.contract(address=config.LINEA_TOKEN_ADDRESS, abi=token_abi)
            balance = token_contract.functions.balanceOf(address).call()
            return balance
        except Exception as e:
            logger.warning(f"Balance check failed on {rpc}: {e}")
    logger.error(f"Failed to fetch balance for {shorten_address(address)} on all RPCs")
    return -1


def check_allocation(wallet: Any, proxy: Optional[str], wallet_index: int) -> int:
    """Checks token allocation for the wallet using calculateAllocation."""
    claim_abi = load_abi(config.CLAIM_CONTRACT_ABI_PATH)
    wallet_short = shorten_address(wallet.address)
    for rpc in config.RPC_LIST:
        w3 = get_w3_with_retry(rpc, proxy)
        if w3 is None:
            logger.warning(f"Failed to connect to RPC {rpc} for allocation check")
            continue
        try:
            chain_id = w3.eth.chain_id
            logger.debug(f"Checking allocation on {rpc}, chain_id: {chain_id}")
            claim_contract = w3.eth.contract(address=config.CLAIM_CONTRACT_ADDRESS, abi=claim_abi)
            allocation = claim_contract.functions.calculateAllocation(wallet.address).call()
            logger.info(f"Wallet #{wallet_index} ({wallet_short}): Allocation = {allocation / 1e18:.6f} $LINEA")
            return allocation
        except Exception as e:
            logger.warning(f"Allocation check failed on {rpc}: {e}")
    logger.error(f"Failed to check allocation for {wallet_short} on all RPCs")
    return -1


def wait_for_contract_balance(proxy: Optional[str], wallet_index: int, wallet_short: str) -> bool:
    """Waits indefinitely for the claim contract to have $LINEA tokens."""
    while True:
        balance = get_token_balance(config.CLAIM_CONTRACT_ADDRESS, proxy)
        if balance == -1:
            logger.error(f"Wallet #{wallet_index} ({wallet_short}): Failed to check contract balance")
            return False
        if balance > 0:
            logger.info(
                f"Wallet #{wallet_index} ({wallet_short}): Contract has {balance / 1e18:.6f} $LINEA, proceeding with claim")
            return True
        logger.warning(
            f"Wallet #{wallet_index} ({wallet_short}): Contract balance is 0, waiting {config.CHECK_INTERVAL} seconds...")
        time.sleep(config.CHECK_INTERVAL)


def sync_send_transaction(
        wallet: Any,
        txn_builder: Callable[[Any, int, int, int], Dict[str, Any]],
        proxy: Optional[str],
        timeout: int = config.TX_TIMEOUT
) -> tuple[bool, Optional[str]]:
    """Sends a transaction with RPC retries, returns (success, tx_hash)."""
    for rpc in config.RPC_LIST:
        w3 = get_w3_with_retry(rpc, proxy)
        if w3 is None:
            logger.error(f"Failed to connect to RPC {rpc}")
            continue
        try:
            chain_id = w3.eth.chain_id
            logger.debug(f"Connected to RPC {rpc}, chain_id: {chain_id}")
        except Exception as e:
            logger.error(f"Failed to get chain_id from {rpc}: {e}")
            continue
        eth = Eth(w3)
        for attempt in range(1, config.RPC_TRY + 1):
            try:
                eth_balance = eth.get_balance(wallet.address)
                logger.debug(f"Wallet {shorten_address(wallet.address)}: ETH balance = {eth_balance / 1e18:.6f} ETH")
                if eth_balance < 1e15:  # 0.001 ETH
                    logger.warning(
                        f"Wallet {shorten_address(wallet.address)}: Low ETH balance ({eth_balance / 1e18:.6f} ETH)")

                nonce = eth.get_transaction_count(wallet.address)
                logger.debug(
                    f"Wallet {shorten_address(wallet.address)}: Attempt {attempt}/{config.RPC_TRY}, Nonce = {nonce}")
                gas_price = int(w3.eth.gas_price * config.GAS_PRICE_MULTIPLIER)
                logger.debug(f"Gas price: {gas_price} wei")
                temp_txn = txn_builder(w3, nonce, gas_price, 1_000_000)
                logger.debug(f"Transaction built: {temp_txn}")
                try:
                    gas_limit = eth.estimate_gas(temp_txn)
                    gas_limit = int(gas_limit * 1.2)
                    logger.debug(f"Estimated gas: {gas_limit}")
                except Exception as e:
                    logger.warning(f"Attempt {attempt}/{config.RPC_TRY} failed on {rpc}: Gas estimation failed: {e}")
                    if str(e).startswith("execution reverted"):
                        logger.error(f"Contract reverted: {e}")
                    continue
                txn = txn_builder(w3, nonce, gas_price, gas_limit)
                txn["chainId"] = chain_id
                logger.debug(f"Final transaction: {txn}")
                try:
                    signed_txn = w3.eth.account.sign_transaction(txn, wallet.key)
                    logger.debug(f"Signed transaction hash: {signed_txn.hash.hex()}")
                    logger.debug(f"Raw transaction: {signed_txn.raw_transaction.hex()}")
                except Exception as e:
                    logger.warning(f"Attempt {attempt}/{config.RPC_TRY} failed on {rpc}: Sign transaction failed: {e}")
                    continue
                try:
                    tx_hash = eth.send_raw_transaction(signed_txn.raw_transaction)
                    logger.info(f"Transaction sent ({shorten_address(wallet.address)}) [{rpc}], tx: {tx_hash.hex()}")
                except Exception as e:
                    logger.warning(f"Attempt {attempt}/{config.RPC_TRY} failed on {rpc}: Send transaction failed: {e}")
                    continue
                try:
                    receipt = eth.wait_for_transaction_receipt(tx_hash, timeout=timeout)
                    if receipt.status == 1:
                        logger.info(
                            f"Transaction successful ({shorten_address(wallet.address)}) [{rpc}], tx: {tx_hash.hex()}")
                        return True, tx_hash.hex()
                    logger.warning(
                        f"Transaction reverted ({shorten_address(wallet.address)}) [{rpc}], tx: {tx_hash.hex()}")
                    return False, tx_hash.hex()
                except Exception as e:
                    logger.warning(f"Attempt {attempt}/{config.RPC_TRY} failed on {rpc}: Wait for receipt failed: {e}")
                    continue
            except Exception as e:
                logger.warning(f"Attempt {attempt}/{config.RPC_TRY} failed on {rpc}: General error: {e}")
        logger.error(f"RPC {rpc} unavailable, switching...")
    return False, None


def check_has_claimed(wallet: Any, proxy: Optional[str], wallet_index: int) -> Optional[bool]:
    """Checks if wallet has already claimed, returns first successful result."""
    claim_abi = load_abi(config.CLAIM_CONTRACT_ABI_PATH)
    wallet_short = shorten_address(wallet.address)
    for rpc in config.RPC_LIST:
        w3 = get_w3_with_retry(rpc, proxy)
        if w3 is None:
            logger.warning(f"Failed to connect to RPC {rpc} for hasClaimed check")
            continue
        try:
            chain_id = w3.eth.chain_id
            logger.debug(f"Checking hasClaimed on {rpc}, chain_id: {chain_id}")
            claim_contract = w3.eth.contract(address=config.CLAIM_CONTRACT_ADDRESS, abi=claim_abi)
            has_claimed = claim_contract.functions.hasClaimed(wallet.address).call()
            logger.info(f"Wallet #{wallet_index} ({wallet_short}): hasClaimed = {has_claimed}")
            return has_claimed
        except Exception as e:
            logger.warning(f"hasClaimed check failed on {rpc}: {e}")
    logger.error(f"Failed to check hasClaimed for {wallet_short} on all RPCs")
    return None


def sync_claim_only(wallet: Any, proxy: Optional[str], wallet_index: int, attempt: int = 1) -> bool:
    """Performs only claim for a wallet with retries."""
    claim_abi = load_abi(config.CLAIM_CONTRACT_ABI_PATH)
    wallet_short = shorten_address(wallet.address)

    # Check if already claimed
    has_claimed = check_has_claimed(wallet, proxy, wallet_index)
    if has_claimed is None:
        logger.error(f"Wallet #{wallet_index} ({wallet_short}): Failed to check hasClaimed on all RPCs")
        return False
    if has_claimed:
        logger.info(f"Wallet #{wallet_index} ({wallet_short}): Already claimed, skipping")
        return True

    # Check allocation
    allocation = check_allocation(wallet, proxy, wallet_index)
    if allocation == -1:
        logger.error(f"Wallet #{wallet_index} ({wallet_short}): Failed to check allocation")
        return False
    if allocation == 0:
        logger.warning(f"Wallet #{wallet_index} ({wallet_short}): No allocation available, skipping")
        with open("failed_wallets.txt", "a") as f:
            f.write(f"{wallet.address},,no_allocation,{proxy or ''}\n")
        return False

    # Wait for contract to have tokens
    if not wait_for_contract_balance(proxy, wallet_index, wallet_short):
        logger.error(f"Wallet #{wallet_index} ({wallet_short}): Cannot proceed, contract balance check failed")
        with open("failed_wallets.txt", "a") as f:
            f.write(f"{wallet.address},,contract_empty,{proxy or ''}\n")
        return False

    # Claim
    logger.info(f"Wallet #{wallet_index} ({wallet_short}): Starting claim (attempt {attempt}/{config.WALLET_RETRIES})")
    claim_tx_builder = lambda web3_instance, n, gp, gl: web3_instance.eth.contract(
        address=config.CLAIM_CONTRACT_ADDRESS, abi=claim_abi).functions.claim().build_transaction({
        "from": wallet.address, "nonce": n, "gas": gl, "gasPrice": gp, "chainId": web3_instance.eth.chain_id
    })

    claim_success, claim_tx_hash = sync_send_transaction(wallet, claim_tx_builder, proxy)
    if not claim_success:
        logger.error(f"Wallet #{wallet_index} ({wallet_short}): Claim failed, tx: {claim_tx_hash or 'None'}")
        if attempt < config.WALLET_RETRIES:
            logger.info(
                f"Wallet #{wallet_index} ({wallet_short}): Retrying claim in {config.RETRY_DELAY_SEC} seconds...")
            time.sleep(config.RETRY_DELAY_SEC)
            return sync_claim_only(wallet, proxy, wallet_index, attempt + 1)
        logger.error(f"Wallet #{wallet_index} ({wallet_short}): All claim attempts failed")
        with open("failed_wallets.txt", "a") as f:
            f.write(f"{wallet.address},,claim_only,{proxy or ''}\n")
        return False

    logger.info(f"Wallet #{wallet_index} ({wallet_short}): Claim successful")
    return True


def sync_claim_and_transfer(wallet: Any, deposit_address: str, proxy: Optional[str], wallet_index: int,
                            attempt: int = 1) -> bool:
    """Performs claim and transfer for a wallet with retries."""
    claim_abi = load_abi(config.CLAIM_CONTRACT_ABI_PATH)
    wallet_short = shorten_address(wallet.address)

    # Check if already claimed
    has_claimed = check_has_claimed(wallet, proxy, wallet_index)
    if has_claimed is None:
        logger.error(f"Wallet #{wallet_index} ({wallet_short}): Failed to check hasClaimed on all RPCs")
        return False
    if has_claimed:
        logger.info(f"Wallet #{wallet_index} ({wallet_short}): Already claimed, skipping")
        return True

    # Check allocation
    allocation = check_allocation(wallet, proxy, wallet_index)
    if allocation == -1:
        logger.error(f"Wallet #{wallet_index} ({wallet_short}): Failed to check allocation")
        return False
    if allocation == 0:
        logger.warning(f"Wallet #{wallet_index} ({wallet_short}): No allocation available, skipping")
        with open("failed_wallets.txt", "a") as f:
            f.write(f"{wallet.address},{deposit_address},no_allocation,{proxy or ''}\n")
        return False

    # Wait for contract to have tokens
    if not wait_for_contract_balance(proxy, wallet_index, wallet_short):
        logger.error(f"Wallet #{wallet_index} ({wallet_short}): Cannot proceed, contract balance check failed")
        with open("failed_wallets.txt", "a") as f:
            f.write(f"{wallet.address},{deposit_address},contract_empty,{proxy or ''}\n")
        return False

    # Claim
    logger.info(f"Wallet #{wallet_index} ({wallet_short}): Starting claim (attempt {attempt}/{config.WALLET_RETRIES})")
    claim_tx_builder = lambda web3_instance, n, gp, gl: web3_instance.eth.contract(
        address=config.CLAIM_CONTRACT_ADDRESS, abi=claim_abi).functions.claim().build_transaction({
        "from": wallet.address, "nonce": n, "gas": gl, "gasPrice": gp, "chainId": web3_instance.eth.chain_id
    })

    claim_success, claim_tx_hash = sync_send_transaction(wallet, claim_tx_builder, proxy)
    if not claim_success:
        logger.error(f"Wallet #{wallet_index} ({wallet_short}): Claim failed, tx: {claim_tx_hash or 'None'}")
        if attempt < config.WALLET_RETRIES:
            logger.info(
                f"Wallet #{wallet_index} ({wallet_short}): Retrying claim in {config.RETRY_DELAY_SEC} seconds...")
            time.sleep(config.RETRY_DELAY_SEC)
            return sync_claim_and_transfer(wallet, deposit_address, proxy, wallet_index, attempt + 1)
        logger.error(f"Wallet #{wallet_index} ({wallet_short}): All claim attempts failed")
        with open("failed_wallets.txt", "a") as f:
            f.write(f"{wallet.address},{deposit_address},claim_and_transfer,{proxy or ''}\n")
        return False

    logger.info(f"Wallet #{wallet_index} ({wallet_short}): Claim successful, initiating transfer")

    # Balance check
    balance = get_token_balance(wallet.address, proxy)
    if balance == -1:
        logger.error(f"Wallet #{wallet_index} ({wallet_short}): Failed to fetch $LINEA balance")
        return False
    if balance == 0:
        logger.warning(f"Wallet #{wallet_index} ({wallet_short}): Claimed 0 $LINEA tokens")
        return False

    # Transfer
    token_abi = load_abi(config.ERC20_ABI_PATH)
    transfer_tx_builder = lambda web3_instance, n, gp, gl: web3_instance.eth.contract(
        address=config.LINEA_TOKEN_ADDRESS, abi=token_abi).functions.transfer(deposit_address,
                                                                              balance).build_transaction({
        "from": wallet.address, "nonce": n, "gas": gl, "gasPrice": gp, "chainId": web3_instance.eth.chain_id
    })

    transfer_success, transfer_tx_hash = sync_send_transaction(wallet, transfer_tx_builder, proxy)
    if not transfer_success:
        logger.error(f"Wallet #{wallet_index} ({wallet_short}): Transfer failed, tx: {transfer_tx_hash or 'None'}")
        if attempt < config.WALLET_RETRIES:
            logger.info(
                f"Wallet #{wallet_index} ({wallet_short}): Retrying transfer in {config.RETRY_DELAY_SEC} seconds...")
            time.sleep(config.RETRY_DELAY_SEC)
            return sync_claim_and_transfer(wallet, deposit_address, proxy, wallet_index, attempt + 1)
        logger.error(f"Wallet #{wallet_index} ({wallet_short}): All transfer attempts failed")
        with open("failed_wallets.txt", "a") as f:
            f.write(f"{wallet.address},{deposit_address},claim_and_transfer,{proxy or ''}\n")
        return False

    logger.info(
        f"Wallet #{wallet_index} ({wallet_short}): Transfer completed, {balance} $LINEA, tx: {transfer_tx_hash}")
    return True


async def claim_only(wallet: Any, proxy: Optional[str], wallet_index: int) -> bool:
    """Async wrapper for claim_only."""
    return await asyncio.to_thread(sync_claim_only, wallet, proxy, wallet_index)


async def claim_and_transfer(wallet: Any, deposit_address: str, proxy: Optional[str], wallet_index: int) -> bool:
    """Async wrapper for claim_and_transfer."""
    return await asyncio.to_thread(sync_claim_and_transfer, wallet, deposit_address, proxy, wallet_index)


async def worker(wallet_data: Dict[str, Any], index: int, claim_only_mode: bool = False) -> None:
    """Processes a single wallet for claim or claim+transfer."""
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
        await claim_only(wallet, proxy, index)
    else:
        await claim_and_transfer(wallet, deposit_address, proxy, index)


async def run_claim_workers(claim_only_mode: bool = False) -> None:
    """Runs claim workers for all wallets in batches."""
    wallets = load_wallets(config.EXCEL_PATH)
    logger.info(f"Loaded {len(wallets)} wallets for {'claiming' if claim_only_mode else 'claiming and transferring'}")

    batch_size = config.WALLETS_IN_WORK
    for batch_start in range(0, len(wallets), batch_size):
        batch = wallets[batch_start:batch_start + batch_size]
        logger.info(
            f"Processing batch {batch_start // batch_size + 1} (wallets #{batch_start + 1} to #{min(batch_start + batch_size, len(wallets))})")

        # Log scheduling for all wallets in the batch
        for i, wallet_data in enumerate(batch):
            try:
                private_key = wallet_data["private_key"]
                validate_private_key(private_key)
                wallet = Account.from_key(private_key)
                wallet_short = shorten_address(wallet.address)
                delay = i * config.SLEEP_BETWEEN_WALLETS_SEC
                logger.info(
                    f"Wallet #{batch_start + i + 1} ({wallet_short}): Scheduled to {'claim' if claim_only_mode else 'claim and transfer'} in {'Now' if delay == 0 else f'{delay} seconds'}")
            except Exception as e:
                logger.error(f"Wallet #{batch_start + i + 1}: Invalid private key: {e}")
                continue

        sem = asyncio.Semaphore(config.WALLETS_IN_WORK) if config.ASYNC_MODE else None

        async def sem_task(wallet_data: Dict[str, Any], index: int, delay: int) -> None:
            await asyncio.sleep(delay)
            if sem:
                async with sem:
                    await worker(wallet_data, index, claim_only_mode)
            else:
                await worker(wallet_data, index, claim_only_mode)

        tasks = [
            asyncio.create_task(sem_task(wallet_data, batch_start + i + 1, i * config.SLEEP_BETWEEN_WALLETS_SEC))
            for i, wallet_data in enumerate(batch)
            if validate_private_key(wallet_data["private_key"]) is None  # Skip invalid keys
        ]

        await asyncio.gather(*tasks)
        logger.info(f"Batch {batch_start // batch_size + 1} completed")


async def main() -> None:
    """Main entry point."""
    await run_claim_workers()