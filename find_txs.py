import psycopg2
import sqlite3
import time
from datetime import datetime

# Add this after the imports
def adapt_datetime(dt):
    return dt.isoformat()

def convert_datetime(s):
    return datetime.fromisoformat(s)

# Register the adapter and converter
sqlite3.register_adapter(datetime, adapt_datetime)
sqlite3.register_converter("timestamp", convert_datetime)

# Database connection parameters
db_params = {
    'host': 'localhost',  # Replace with your DB host
    'dbname': 'cexplorer',
    'user': 'user',
    'password': 'password',
}

# Define the epoch threshold
epoch_threshold = 524  # Starting epoch number if database does not exist

# Add after database parameters
matched_addresses = set()  # Store matched addresses globally

def init_local_db():
    """Initialize local SQLite database"""
    conn = sqlite3.connect('local_transactions.db')
    cursor = conn.cursor()
    
    cursor.execute('''
        CREATE TABLE IF NOT EXISTS tx (
            tx_hash TEXT PRIMARY KEY,
            tx_date TIMESTAMP,
            metadata TEXT,
            target_ada_input REAL,
            target_ada_output REAL,
            match_ada_input REAL,
            match_ada_output REAL,
            target_address TEXT,
            matched_address TEXT,
            inputs TEXT,     -- JSON string of inputs [{address, amount}, ...]
            outputs TEXT,    -- JSON string of outputs [{address, amount}, ...]
            tx_type TEXT     -- CREATION, INCREASE, DECREASE, or DELETION
        )
    ''')
    
    conn.commit()
    return conn

def get_latest_transaction_date(sqlite_conn):
    """Get the latest transaction date from local SQLite database"""
    cursor = sqlite_conn.cursor()
    cursor.execute("SELECT MAX(tx_date) FROM tx")
    result = cursor.fetchone()
    return result[0] if result and result[0] else None

def get_latest_processed_date():
    """Get the latest processed date from a tracking file"""
    try:
        with open('latest_processed_date.txt', 'r') as f:
            date_str = f.read().strip()
            return datetime.fromisoformat(date_str)
    except:
        return None

def save_latest_processed_date(date):
    """Save the latest processed date to a tracking file"""
    if isinstance(date, datetime):
        date_str = date.isoformat()
    else:
        date_str = date
    with open('latest_processed_date.txt', 'w') as f:
        f.write(date_str)

def determine_tx_type(target_ada_input, target_ada_output):
    """Determine transaction type based on wallet input/output values"""
    if target_ada_input == 0 and target_ada_output > 0:
        return "CREATION"
    elif target_ada_input > 0 and target_ada_output == 0:
        return "DELETION"
    elif target_ada_input > target_ada_output:
        return "DECREASE"
    elif target_ada_output > target_ada_input:
        return "INCREASE"
    else:
        return "UNKNOWN"

# Initial setup - before the loop
sqlite_conn = init_local_db()
latest_processed_date = None

# Check if we have any transactions in the SQLite database
sqlite_cursor = sqlite_conn.cursor()
sqlite_cursor.execute("SELECT MAX(tx_date) FROM tx")
db_latest_date = sqlite_cursor.fetchone()[0]

if db_latest_date:
    latest_processed_date = db_latest_date
    print(f"Resuming from last saved transaction date: {latest_processed_date}")
else:
    print("No transactions in database, starting from epoch threshold")

while True:
    try:
        # Read target address and policy ID
        with open('files/wallet.addr', 'r') as file:
            target_address = file.read().strip()
            # print(f"\nTarget address: {target_address}")
        with open('files/policy.id', 'r') as file:
            target_policyid = file.read().strip()
            # print(f"Target policy ID: {target_policyid}")
        
        # Connect to PostgreSQL
        pg_conn = psycopg2.connect(**db_params)
        pg_cursor = pg_conn.cursor()

        # Check if we have any transactions in the SQLite database
        sqlite_cursor = sqlite_conn.cursor()
        sqlite_cursor.execute("SELECT COUNT(*) FROM tx")
        has_records = sqlite_cursor.fetchone()[0] > 0

        if not has_records and latest_processed_date is None:
            query = """
            SELECT DISTINCT encode(tx.hash, 'hex') as tx_hash, block.time as tx_date
            FROM tx_out
            JOIN tx ON tx.id = tx_out.tx_id
            JOIN block ON block.id = tx.block_id
            JOIN tx_in ON tx_in.tx_in_id = tx.id
            JOIN tx_out source_tx_out ON tx_in.tx_out_id = source_tx_out.tx_id 
                AND tx_in.tx_out_index = source_tx_out.index
            WHERE (
                (source_tx_out.address = %s)  -- wallet address in inputs
                OR 
                (tx_out.address = %s)  -- wallet address in outputs
                OR 
                (tx_out.address = ANY(%s))  -- matched addresses in outputs
            )
            AND block.epoch_no >= %s
            ORDER BY block.time ASC;
            """
            pg_cursor.execute(query, (target_address, target_address, list(matched_addresses), epoch_threshold))
        else:
            query = """
            SELECT DISTINCT encode(tx.hash, 'hex') as tx_hash, block.time as tx_date
            FROM tx_out
            JOIN tx ON tx.id = tx_out.tx_id
            JOIN block ON block.id = tx.block_id
            JOIN tx_in ON tx_in.tx_in_id = tx.id
            JOIN tx_out source_tx_out ON tx_in.tx_out_id = source_tx_out.tx_id 
                AND tx_in.tx_out_index = source_tx_out.index
            WHERE (
                (source_tx_out.address = %s)  -- wallet address in inputs
                OR 
                (tx_out.address = %s)  -- wallet address in outputs
                OR 
                (tx_out.address = ANY(%s))  -- matched addresses in outputs
            )
            AND block.time > %s
            ORDER BY block.time ASC;
            """
            pg_cursor.execute(query, (target_address, target_address, list(matched_addresses), latest_processed_date))

        # Process new transactions
        new_transactions = pg_cursor.fetchall()
        # print(f"Found {len(new_transactions)} new transactions")
        
        if new_transactions:
            # Update the latest date we've seen
            latest_processed_date = new_transactions[-1][1]
            print(f"Updated latest processed date to: {latest_processed_date}")
            
            transactions_saved = 0
            
            for row in new_transactions:
                tx_hash, tx_date = row
                print(f"\n---Processing transaction: {tx_hash} / {tx_date}")

                # Get all inputs for this transaction
                inputs_query = """
                SELECT tx_in.tx_out_id, tx_in.tx_out_index, tx_out.address, tx_out.value
                FROM tx_in
                INNER JOIN tx_out ON tx_in.tx_out_id = tx_out.tx_id AND tx_in.tx_out_index = tx_out.index
                INNER JOIN tx ON tx.id = tx_in.tx_in_id
                WHERE tx.hash = decode(%s, 'hex');
                """
                pg_cursor.execute(inputs_query, (tx_hash,))
                inputs = pg_cursor.fetchall()

                # Get all outputs for this transaction
                outputs_query = """
                SELECT tx_out.address, tx_out.value
                FROM tx_out
                INNER JOIN tx ON tx_out.tx_id = tx.id
                WHERE tx.hash = decode(%s, 'hex');
                """
                pg_cursor.execute(outputs_query, (tx_hash,))
                outputs = pg_cursor.fetchall()

                # Print all inputs together
                print("\nInputs:")
                for _, _, addr, value in inputs:
                    ada_value = float(value) / 1000000.0
                    if addr == target_address:
                        print(f"  → {ada_value:,.6f} ADA from $me")
                    else:
                        truncated_addr = f"{addr[:4]}...{addr[-7:]}"
                        print(f"  → {ada_value:,.6f} ADA from {truncated_addr}")

                # Print all outputs together
                print("\nOutputs:")
                for addr, value in outputs:
                    ada_value = float(value) / 1000000.0
                    if addr == target_address:
                        print(f"  ← {ada_value:,.6f} ADA to $me")
                    else:
                        truncated_addr = f"{addr[:7]}...{addr[-7:]}"
                        print(f"  ← {ada_value:,.6f} ADA to {truncated_addr}")

                # Calculate totals for database storage (since schema remains unchanged)
                target_ada_input = sum(float(value) / 1000000.0 for _, _, addr, value in inputs if addr == target_address)
                target_ada_output = sum(float(value) / 1000000.0 for addr, value in outputs if addr == target_address)
                match_ada_input = sum(float(value) / 1000000.0 for _, _, addr, value in inputs if addr != target_address)
                match_ada_output = sum(float(value) / 1000000.0 for addr, value in outputs if addr != target_address)

                # Fetch metadata
                metadata_query = """
                SELECT key, json
                FROM tx_metadata
                JOIN tx ON tx.id = tx_metadata.tx_id
                WHERE tx.hash = decode(%s, 'hex');
                """
                pg_cursor.execute(metadata_query, (tx_hash,))
                metadata_result = pg_cursor.fetchall()
                
                # Process metadata
                cleaned_policyid = None
                metadata = {}
                if metadata_result:
                    for key, json_data in metadata_result:
                        metadata[key] = json_data
                        if isinstance(json_data, str) and '::' in str(json_data):
                            try:
                                parts = str(json_data).split('::')
                                if len(parts) > 1:
                                    cleaned_policyid = parts[0].strip('"')
                            except Exception as e:
                                print(f"Error processing metadata: {e}")

                print(f"Metadata found: {bool(metadata_result)}")
                print(f"Cleaned Policy ID: {cleaned_policyid}")
                print(f"Target Policy ID: {target_policyid}")
                
                # Save to database only if policy ID matches
                if cleaned_policyid == target_policyid:
                    try:
                        # Add matched addresses to our tracking set
                        for _, _, addr, _ in inputs:
                            if addr != target_address:
                                matched_addresses.add(addr)
                        for addr, _ in outputs:
                            if addr != target_address:
                                matched_addresses.add(addr)
                                
                        # Format inputs and outputs as JSON strings
                        inputs_json = [
                            {
                                "address": addr,
                                "amount": float(value) / 1000000.0
                            }
                            for _, _, addr, value in inputs
                        ]
                        
                        outputs_json = [
                            {
                                "address": addr,
                                "amount": float(value) / 1000000.0
                            }
                            for addr, value in outputs
                        ]

                        # Add tx_type determination before the INSERT
                        tx_type = determine_tx_type(target_ada_input, target_ada_output)

                        sqlite_cursor.execute(
                            """INSERT INTO tx (
                                tx_hash, tx_date, metadata,
                                target_ada_input, target_ada_output,
                                match_ada_input, match_ada_output,
                                target_address, matched_address,
                                inputs, outputs, tx_type
                            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                            (tx_hash, tx_date, str(metadata),
                             target_ada_input, target_ada_output,
                             match_ada_input, match_ada_output,
                             target_address, ','.join(addr for addr, _ in outputs if addr != target_address),
                             str(inputs_json), str(outputs_json), tx_type)
                        )
                        transactions_saved += 1
                        print(f"✓ Policy ID matches! Saving transaction data")
                        print(f"Added matched addresses: {[addr for addr, _ in outputs if addr != target_address]}")
                    except sqlite3.Error as e:
                        print(f"SQLite error: {e}")
                else:
                    # Check if metadata contains policy ID format
                    has_policy_format = False
                    if metadata_result:
                        for key, json_data in metadata_result:
                            if isinstance(json_data, str) and '::' in str(json_data):
                                has_policy_format = True
                                break
                    
                    if has_policy_format:
                        print(f"✗ Transaction has policy ID format but doesn't match target - skipping")
                    else:
                        # Check if wallet address is in inputs AND matched address is in outputs
                        has_wallet_in_inputs = any(addr == target_address for _, _, addr, _ in inputs)
                        has_matched_in_outputs = any(addr in matched_addresses for addr, _ in outputs)
                        
                        if has_wallet_in_inputs and has_matched_in_outputs:
                            # Check datum for policy ID before saving
                            datum_query = """
                            SELECT encode(d.bytes, 'hex') as datum_bytes
                            FROM tx
                            JOIN tx_in ON tx_in.tx_in_id = tx.id
                            JOIN tx_out source_tx_out ON tx_in.tx_out_id = source_tx_out.tx_id 
                                AND tx_in.tx_out_index = source_tx_out.index
                            JOIN datum d ON d.hash = source_tx_out.data_hash
                            WHERE tx.hash = decode(%s, 'hex')
                            """
                            
                            pg_cursor.execute(datum_query, (tx_hash,))
                            datum_results = pg_cursor.fetchall()
                            
                            found_in_datum = False
                            if datum_results:
                                for datum_row in datum_results:
                                    datum_bytes = datum_row[0]
                                    if target_policyid in datum_bytes:
                                        found_in_datum = True
                                        print(f"✓ Found policy ID in transaction datum! Saving transaction data")
                                        break
                            
                            if found_in_datum:
                                try:
                                    # Format inputs and outputs as JSON strings
                                    inputs_json = [
                                        {
                                            "address": addr,
                                            "amount": float(value) / 1000000.0
                                        }
                                        for _, _, addr, value in inputs
                                    ]
                                    
                                    outputs_json = [
                                        {
                                            "address": addr,
                                            "amount": float(value) / 1000000.0
                                        }
                                        for addr, value in outputs
                                    ]

                                    # Add tx_type determination before the INSERT
                                    tx_type = determine_tx_type(target_ada_input, target_ada_output)

                                    sqlite_cursor.execute(
                                        """INSERT INTO tx (
                                            tx_hash, tx_date, metadata,
                                            target_ada_input, target_ada_output,
                                            match_ada_input, match_ada_output,
                                            target_address, matched_address,
                                            inputs, outputs, tx_type
                                        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)""",
                                        (tx_hash, tx_date, str(metadata),
                                         target_ada_input, target_ada_output,
                                         match_ada_input, match_ada_output,
                                         target_address, ','.join(addr for addr, _ in outputs if addr != target_address),
                                         str(inputs_json), str(outputs_json), tx_type)
                                    )
                                    transactions_saved += 1
                                except sqlite3.Error as e:
                                    print(f"SQLite error: {e}")
                            else:
                                print(f"✗ Related transaction found but policy ID not in datum - skipping")
                        else:
                            print(f"✗ No policy ID format and no matching pattern - skipping transaction")

            sqlite_conn.commit()
            print(f"\nProcessed up to date: {latest_processed_date}")
            print(f"Saved {transactions_saved} matching transactions out of {len(new_transactions)} total transactions")

        # else:
        #     print("No new transactions found")
            
        # Clean up connections
        pg_cursor.close()
        pg_conn.close()

        time.sleep(10)

    except Exception as e:
        print(f"Error: {e}")
        time.sleep(1)


