import sqlite3
import time
from datetime import datetime
import os
import requests
import json
import ast

# Update constants to read from files
try:
    with open('files/telegram.token', 'r') as f:
        BOT_TOKEN = f.read().strip()
    with open('files/user.id', 'r') as f:
        CHAT_ID = f.read().strip()
    with open('files/wallet.addr', 'r') as f:
        WALLET_ADDR = f.read().strip()
except FileNotFoundError as e:
    print(f"Error: Required file not found - {e}")
    exit(1)

def send_telegram_message(message):
    try:
        url = f"https://api.telegram.org/bot{BOT_TOKEN}/sendMessage"
        payload = {
            "chat_id": CHAT_ID,
            "text": message,
            "parse_mode": "HTML",
            "disable_web_page_preview": True
        }
        response = requests.post(url, json=payload)
        response.raise_for_status()
    except Exception as e:
        print(f"Error sending Telegram message: {e}")

def truncate_address(address):
    if len(address) > 15:
        return f"{address[:10]}...{address[-10:]}"
    return address

def format_json_field(json_str):
    try:
        data = ast.literal_eval(json_str)
        
        formatted_str = ""
        for item in data:
            if isinstance(item, dict):
                if 'address' in item and 'amount' in item:
                    # Use $YOU for your wallet, truncate other addresses
                    display_address = "$me" if item['address'] == WALLET_ADDR else truncate_address(item['address'])
                    # formatted_str += (
                    #     f"📍 <b>Address:</b> <code>{display_address}</code>\n"
                    #     f"💰 <b>Amount:</b> {item['amount']:.6f} ADA\n"
                    #     "──────────────\n"
                    # )
                    formatted_str += (
                    f"📍<code>{display_address}</code>\n"
                    f"💰 {item['amount']:.6f} ADA\n"
                    "──────────────\n"
                    )
        return formatted_str.strip() if formatted_str else "No data available"
    except Exception as e:
        print(f"Error: {e}")
        print(f"Problematic string: {json_str}")
        return f"Error parsing data: {str(e)}"

def get_latest_transaction(db_path):
    try:
        conn = sqlite3.connect(db_path)
        cursor = conn.cursor()
        
        # Updated query to include tx_type
        query = """
        SELECT tx_hash, tx_date, target_ada_input, target_ada_output, inputs, outputs, tx_type 
        FROM tx 
        ORDER BY tx_date DESC 
        LIMIT 1
        """
        
        cursor.execute(query)
        result = cursor.fetchone()
        
        if result:
            latest_tx = {
                'tx_hash': result[0],
                'tx_date': result[1],
                'target_ada_input': result[2],
                'target_ada_output': result[3],
                'inputs': result[4],
                'outputs': result[5],
                'tx_type': result[6]
            }
            return latest_tx
        
        return None
        
    except sqlite3.Error as e:
        print(f"Database error: {e}")
        return None
    finally:
        conn.close()

def monitor_database(db_path, check_interval=10):
    last_modified = 0
    latest_tx = None
    
    print(f"Monitoring database at: {db_path}")
    send_telegram_message("🔄 Bot started monitoring transactions")
    
    while True:
        try:
            current_modified = os.path.getmtime(db_path)
            
            if current_modified > last_modified:
                print("\nDatabase change detected!")
                new_tx = get_latest_transaction(db_path)
                
                if new_tx != latest_tx:
                    latest_tx = new_tx
                    # Convert the date string to datetime object
                    tx_date = datetime.strptime(latest_tx['tx_date'], '%Y-%m-%dT%H:%M:%S')
                    # Format UTC+0
                    utc_date = tx_date.strftime('%d/%m/%Y %H:%M:%S')
                    # Calculate UTC-3
                    utc_minus_3 = tx_date.replace(hour=(tx_date.hour - 3) % 24)
                    utc_minus_3_date = utc_minus_3.strftime('%H:%M:%S')
                    
                    message = (
                        f"{utc_date} ({utc_minus_3_date} UTC-3)\n"
                        f"<a href='https://cexplorer.io/tx/{latest_tx['tx_hash']}'>{latest_tx['tx_hash']}</a>\n\n"
                        # f"<b>Transaction Type:</b> {latest_tx['tx_type']}\n"
                        # f"<b>ADA Input........:</b> {latest_tx['target_ada_input']}\n"
                        f"<b>Offer:</b> {latest_tx['target_ada_output']} ADA\n"
                        f"({latest_tx['target_ada_input']} {latest_tx['tx_type']} {latest_tx['target_ada_output'] - latest_tx['target_ada_input']} ADA)\n\n"
                        # f"<b>ADA Difference:</b> {latest_tx['target_ada_output'] - latest_tx['target_ada_input']}\n\n"
                        f"<b>Inputs:</b>\n{format_json_field(latest_tx['inputs'])}\n\n"
                        f"<b>Outputs:</b>\n{format_json_field(latest_tx['outputs'])}"
                    )
                    send_telegram_message(message)
                    print(message)
                
                last_modified = current_modified
            
            time.sleep(check_interval)
            
        except KeyboardInterrupt:
            send_telegram_message("🛑 Bot stopped monitoring")
            print("\nMonitoring stopped by user")
            break
        except Exception as e:
            error_message = f"❌ Error: {str(e)}"
            send_telegram_message(error_message)
            print(f"Error: {e}")
            time.sleep(check_interval)

if __name__ == "__main__":
    # Update database path
    db_path = "local_transactions.db"
    
    # Start monitoring
    monitor_database(db_path)
