#!/usr/bin/env python3
"""
Kuber Session Management Example (v1.7.5)

Demonstrates the CLIENT command for managing Redis protocol sessions:
- Setting and getting session timeout
- Using keepalive for long-running connections
- Naming clients for debugging
- Listing connected clients

Copyright © 2025-2030, All Rights Reserved
Ashutosh Sinha | Email: ajsinha@gmail.com
"""

import redis
import time
import threading
from datetime import datetime


def main():
    """Demonstrate Kuber session management features."""
    
    # Connect to Kuber with API key authentication
    # Replace with your actual API key
    client = redis.Redis(
        host='localhost',
        port=6380,
        password='kub_your_api_key_here',  # Your Kuber API key
        decode_responses=True
    )
    
    print("=" * 60)
    print("Kuber Session Management Example (v1.7.5)")
    print("=" * 60)
    
    try:
        # Test connection
        pong = client.ping()
        print(f"\n✓ Connected to Kuber: {pong}")
        
        # ==================== CLIENT ID ====================
        print("\n--- CLIENT ID ---")
        session_id = client.execute_command('CLIENT', 'ID')
        print(f"Session ID: {session_id}")
        
        # ==================== CLIENT SETNAME / GETNAME ====================
        print("\n--- CLIENT NAME ---")
        client.execute_command('CLIENT', 'SETNAME', 'python-session-demo')
        name = client.execute_command('CLIENT', 'GETNAME')
        print(f"Client name set to: {name}")
        
        # ==================== CLIENT INFO ====================
        print("\n--- CLIENT INFO ---")
        info = client.execute_command('CLIENT', 'INFO')
        print(f"Client info:\n{info}")
        
        # ==================== CLIENT GETTIMEOUT ====================
        print("\n--- SESSION TIMEOUT ---")
        current_timeout = client.execute_command('CLIENT', 'GETTIMEOUT')
        print(f"Current timeout: {current_timeout} seconds")
        
        # ==================== CLIENT SETTIMEOUT ====================
        print("\n--- SETTING LONGER TIMEOUT ---")
        # Set timeout to 1 hour (3600 seconds)
        client.execute_command('CLIENT', 'SETTIMEOUT', '3600')
        new_timeout = client.execute_command('CLIENT', 'GETTIMEOUT')
        print(f"New timeout: {new_timeout} seconds (1 hour)")
        
        # ==================== CLIENT KEEPALIVE ====================
        print("\n--- KEEPALIVE ---")
        response = client.execute_command('CLIENT', 'KEEPALIVE')
        print(f"Keepalive response: {response}")
        
        # ==================== CLIENT LIST ====================
        print("\n--- CONNECTED CLIENTS ---")
        client_list = client.execute_command('CLIENT', 'LIST')
        print("Connected clients:")
        for line in client_list.strip().split('\n'):
            if line:
                print(f"  {line}")
        
        # ==================== CLIENT HELP ====================
        print("\n--- CLIENT HELP ---")
        help_text = client.execute_command('CLIENT', 'HELP')
        print("Available CLIENT subcommands:")
        for cmd in help_text:
            print(f"  {cmd}")
        
        # ==================== DISABLE TIMEOUT EXAMPLE ====================
        print("\n--- PERSISTENT CONNECTION (NO TIMEOUT) ---")
        client.execute_command('CLIENT', 'SETTIMEOUT', '0')
        timeout = client.execute_command('CLIENT', 'GETTIMEOUT')
        print(f"Timeout disabled: {timeout} (0 = no timeout)")
        
        # Restore reasonable timeout
        client.execute_command('CLIENT', 'SETTIMEOUT', '300')
        print("Timeout restored to 300 seconds")
        
        print("\n" + "=" * 60)
        print("Session management demo complete!")
        print("=" * 60)
        
    except redis.exceptions.AuthenticationError as e:
        print(f"✗ Authentication failed: {e}")
        print("  Make sure to use a valid Kuber API key")
    except redis.exceptions.ConnectionError as e:
        print(f"✗ Connection failed: {e}")
        print("  Make sure Kuber server is running on localhost:6380")
    except Exception as e:
        print(f"✗ Error: {e}")
    finally:
        client.close()


def keepalive_thread_example():
    """
    Example of using a background thread to send periodic keepalives.
    This keeps a long-running connection alive even during idle periods.
    """
    
    client = redis.Redis(
        host='localhost',
        port=6380,
        password='kub_your_api_key_here',
        decode_responses=True
    )
    
    # Set a 5-minute timeout
    client.execute_command('CLIENT', 'SETTIMEOUT', '300')
    
    # Flag to stop the keepalive thread
    stop_keepalive = threading.Event()
    
    def send_keepalive():
        """Send keepalive every 60 seconds."""
        while not stop_keepalive.is_set():
            try:
                client.execute_command('CLIENT', 'KEEPALIVE')
                print(f"[{datetime.now().strftime('%H:%M:%S')}] Keepalive sent")
            except Exception as e:
                print(f"Keepalive failed: {e}")
                break
            # Wait 60 seconds or until stop signal
            stop_keepalive.wait(60)
    
    # Start keepalive thread
    keepalive_t = threading.Thread(target=send_keepalive, daemon=True)
    keepalive_t.start()
    print("Keepalive thread started (sending every 60 seconds)")
    
    try:
        # Your main application logic here
        # The connection will stay alive as long as keepalives are sent
        print("Press Ctrl+C to stop...")
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nStopping...")
        stop_keepalive.set()
        keepalive_t.join(timeout=2)
    finally:
        client.close()


def connection_pool_example():
    """
    Example of managing session timeout with connection pools.
    Each connection from the pool gets its own timeout setting.
    """
    
    # Create a connection pool
    pool = redis.ConnectionPool(
        host='localhost',
        port=6380,
        password='kub_your_api_key_here',
        decode_responses=True,
        max_connections=10
    )
    
    # Get a connection and set its timeout
    client = redis.Redis(connection_pool=pool)
    
    # Set a longer timeout for this connection
    client.execute_command('CLIENT', 'SETTIMEOUT', '1800')  # 30 minutes
    client.execute_command('CLIENT', 'SETNAME', 'pool-connection-1')
    
    print(f"Pool connection timeout: {client.execute_command('CLIENT', 'GETTIMEOUT')} seconds")
    
    # Do work...
    client.set('test:key', 'value')
    
    # Connection returns to pool when done
    client.close()
    pool.disconnect()


if __name__ == '__main__':
    main()
    
    # Uncomment to run other examples:
    # keepalive_thread_example()
    # connection_pool_example()
