# Asynchronous mode (True/False)
ASYNC_MODE = True

# Number of concurrent wallets in async mode
WALLETS_IN_WORK = 10

# Delay between wallet starts in seconds (smooth start)
SLEEP_BETWEEN_WALLETS_SEC = 2

CHECK_INTERVAL = 3  # Delay between check $LINEA airdrop contract

WALLET_RETRIES = 3  # NEW: Number of retries per wallet for claim/transfer
RETRY_DELAY_SEC = 5  # NEW: Delay between wallet retries in seconds

# Number of RPC retry attempts
RPC_TRY = 10

# List of RPC endpoints
RPC_LIST = [
    "https://linea.drpc.org",
    "https://rpc.linea.build",
    "https://linea.therpc.io",
]

# Path to Excel file with wallets
EXCEL_PATH = "wallets.xlsx"

# Address of the claim contract
CLAIM_CONTRACT_ADDRESS = "0x87bAa1694381aE3eCaE2660d97fe60404080Eb64"

# Paths to ABI files
CLAIM_CONTRACT_ABI_PATH = "abis/claim_abi.json"
ERC20_ABI_PATH = "abis/erc20_abi.json"

# Address of the $LINEA token contract
LINEA_TOKEN_ADDRESS = "0x1789e0043623282D5DCc7F213d703C6D8BAfBB04"

# Logging level (DEBUG, INFO, WARNING, ERROR, CRITICAL)
LOG_LEVEL = "INFO"

# Use proxies from Excel (True/False)
USE_PROXY = False

# Gas price multiplier for safety
GAS_PRICE_MULTIPLIER = 1.1

# Transaction timeout in seconds
TX_TIMEOUT = 120

# Log file path
LOG_FILE = "claim_log.txt"