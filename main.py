# main.py
import asyncio
import sys
from logger import get_logger
import config
from claim_transfer import run_claim_workers
from utils import get_w3_with_retry

logger = get_logger("Main", config.LOG_LEVEL)

ASCII_BANNER = r"""
  _   _ ______ _________          ______  _____  _  __
 | \ | |  ____|__   __\ \        / / __ \|  __ \| |/ /
 |  \| | |__     | |   \ \  /\  / / |  | | |__) | ' / 
 | . ` |  __|    | |    \ \/  \/ /| |  | |  _  /|  <  
 | |\  | |____   | |     \  /\  / | |__| | | \ \| . \ 
 |_| \_|______|  |_|      \/  \/   \____/|_|  \_\_|\_\
"""

TELEGRAM_LINK = "https://t.me/AltcoinNetwork"

def print_banner() -> None:
    print(ASCII_BANNER)
    print(f"Telegram: {TELEGRAM_LINK}\n")

def menu() -> None:
    print("Select an action:")
    print("1. Start claim and transfer")
    print("2. Start claim only")
    print("3. Check RPC connection")
    print("4. Exit")

async def check_rpc() -> None:
    """Checks availability of RPC nodes and reports chain_id match"""
    print("Checking RPC connectivity...\n")
    for rpc_url in config.RPC_LIST:
        try:
            w3 = get_w3_with_retry(rpc_url, None)
            status = "OK" if w3 and w3.eth.chain_id == config.CHAIN_ID else "FAIL"
            chain_id = w3.eth.chain_id if w3 else "N/A"
            print(f"{rpc_url}: {status} (chain_id: {chain_id})")
        except Exception as e:
            print(f"{rpc_url}: ERROR ({e})")
    print()

async def main_loop() -> None:
    print_banner()
    while True:
        try:
            menu()
            choice = input("Enter action number: ").strip()
            if choice == "1":
                logger.info("Starting claim and transfer process...")
                await run_claim_workers(claim_only_mode=False)
            elif choice == "2":
                logger.info("Starting claim only process...")
                await run_claim_workers(claim_only_mode=True)
            elif choice == "3":
                logger.info("Checking RPC connectivity...")
                await check_rpc()
            elif choice == "4":
                logger.info("Exiting...")
                sys.exit(0)
            else:
                print("Invalid input, please try again.\n")
        except Exception as e:
            logger.error(f"Error in main loop: {e}")
            print("An error occurred, please restart the script.\n")

if __name__ == "__main__":
    asyncio.run(main_loop())
