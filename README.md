# Linea Claim and Transfer

A Python script to automate claiming and transferring $LINEA tokens for multiple wallets.

## Features
- Claims $LINEA tokens from a contract.
- Transfers claimed tokens to a specified CEX deposit address.
- Asynchronous processing with configurable batch sizes and delays.
- Supports proxy usage and RPC retries for reliability.
- Professional logging with console and file output.

## Requirements
- Python 3.8+
- Install dependencies: `pip install -r requirements.txt`
- Excel file (`wallets.xlsx`) with columns: `private_key`, `cex_deposit_address`, (optional) `proxy`.

## Setup
1. Clone the repository:
   ```bash
   git clone https://github.com/UniqueSunrise/Linea-claimer.git
   ```
   ```bash
   cd Linea-claimer
   ```
2. Install dependencies:
    ```bash
    pip install -r requirements.txt
    ```
3. Prepare wallets.xlsx with wallet data. 
Required Columns
- private_key -- The private key of the Ethereum wallet (64-character hexadecimal string starting with 0x).
- cex_deposit_address -- The Ethereum address where claimed $LINEA tokens will be transferred (e.g., a CEX deposit address).
- proxy (optional): HTTP/HTTPS proxy URL (e.g., http://user:pass@proxy:port) if USE_PROXY=True in config.py.

| private_key | cex_deposit_address | proxy                        |
|-------------|---------------------|------------------------------|
| 0x1         |  0x1..deposit       | http://user:pass@proxy:port  |
| 0x2         |  0x2..deposit       | http://user:pass@proxy:port  |


4. Configure config.py with contract addresses and settings.

5. Run the script:
    ```bash
    python main.py
    ```

Usage
- Select "1" to start claiming and transferring.
- Select "2" to check RPC connectivity.
- Select "3" to exit.



