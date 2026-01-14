#!/usr/bin/env python3
"""
Kuber REST API - Insert & Update JSON Documents Demo (v1.7.9)

This standalone script demonstrates how to:

1. INSERT new JSON documents
   - Simple insert
   - Insert with TTL (time to live)
   - Bulk insert

2. UPDATE existing JSON documents
   - Full replace (PUT)
   - Partial update (PATCH/JUPDATE) - merge specific attributes
   - Conditional updates

3. Generic Update API (JUPDATE)
   - Creates new entry if key doesn't exist
   - Merges with existing entry if key exists
   - Deep merge of nested objects

Usage:
    python rest_insert_update_demo.py [--host HOST] [--port PORT] [--api-key KEY]

Example:
    python rest_insert_update_demo.py --host localhost --port 7070 --api-key kub_admin

Copyright (c) 2025-2030 Ashutosh Sinha. All Rights Reserved.
"""

import requests
import json
import argparse
from typing import Dict, List, Any, Optional
from datetime import datetime
import time


class KuberDocumentClient:
    """REST API client for Kuber Document operations (Insert/Update)."""
    
    def __init__(self, host: str = "localhost", port: int = 7070, api_key: str = "kub_admin", use_ssl: bool = False):
        """
        Initialize the Kuber document client.
        
        Args:
            host: Kuber server hostname
            port: Kuber server port
            api_key: API key for authentication
            use_ssl: Use HTTPS instead of HTTP
        """
        protocol = "https" if use_ssl else "http"
        self.base_url = f"{protocol}://{host}:{port}/api/v1"
        self.api_key = api_key
        self.headers = {
            "Content-Type": "application/json",
            "X-API-Key": api_key
        }
    
    # ==================== INSERT Operations ====================
    
    def insert(self, region: str, key: str, document: Dict[str, Any], ttl: int = -1) -> Dict[str, Any]:
        """
        Insert a new JSON document (creates or replaces).
        
        Args:
            region: Cache region
            key: Document key
            document: JSON document to store
            ttl: Time to live in seconds (-1 for no expiration)
            
        Returns:
            API response with status and details
        """
        url = f"{self.base_url}/cache/{region}/{key}"
        params = {}
        if ttl > 0:
            params["ttl"] = ttl
            
        try:
            response = requests.post(url, json=document, headers=self.headers, params=params, timeout=30)
            return {
                "status": response.status_code,
                "success": response.status_code == 200,
                "message": "Created/Updated" if response.status_code == 200 else response.text
            }
        except requests.exceptions.RequestException as e:
            return {"status": 0, "success": False, "error": str(e)}
    
    def bulk_insert(self, region: str, documents: Dict[str, Dict[str, Any]], ttl: int = -1) -> Dict[str, Any]:
        """
        Bulk insert multiple JSON documents.
        
        Args:
            region: Cache region
            documents: Dictionary mapping keys to documents
            ttl: Time to live in seconds (-1 for no expiration)
            
        Returns:
            Summary of bulk operation
        """
        url = f"{self.base_url}/cache/{region}/batch"
        
        # Convert to list format expected by batch API
        batch_data = [{"key": k, "value": v} for k, v in documents.items()]
        
        params = {}
        if ttl > 0:
            params["ttl"] = ttl
            
        try:
            response = requests.post(url, json=batch_data, headers=self.headers, params=params, timeout=60)
            return {
                "status": response.status_code,
                "success": response.status_code == 200,
                "count": len(documents),
                "message": response.text if response.status_code != 200 else "Bulk insert successful"
            }
        except requests.exceptions.RequestException as e:
            return {"status": 0, "success": False, "error": str(e)}
    
    # ==================== UPDATE Operations ====================
    
    def update_full(self, region: str, key: str, document: Dict[str, Any]) -> Dict[str, Any]:
        """
        Full update/replace of a JSON document (same as insert).
        Completely replaces the existing document.
        
        Args:
            region: Cache region
            key: Document key
            document: New JSON document (replaces existing)
            
        Returns:
            API response
        """
        # Full update is same as insert - complete replacement
        return self.insert(region, key, document)
    
    def update_partial(self, region: str, key: str, updates: Dict[str, Any]) -> Dict[str, Any]:
        """
        Partial update using Generic Update API (JUPDATE).
        Merges the updates with the existing document.
        
        - Existing fields not in updates are preserved
        - Fields in updates are added or replaced
        - Nested objects are deep merged
        - If document doesn't exist, creates it
        
        Args:
            region: Cache region
            key: Document key
            updates: Dictionary with attributes to update
            
        Returns:
            API response
        """
        url = f"{self.base_url}/genericupdate"
        
        request_body = {
            "region": region,
            "key": key,
            "value": updates,
            "apiKey": self.api_key
        }
        
        try:
            response = requests.post(url, json=request_body, headers=self.headers, timeout=30)
            result = response.json() if response.content else {}
            return {
                "status": response.status_code,
                "success": response.status_code == 200 and result.get("success", False),
                "action": result.get("action", "unknown"),
                "message": result.get("message", ""),
                "data": result
            }
        except requests.exceptions.RequestException as e:
            return {"status": 0, "success": False, "error": str(e)}
    
    def jupdate(self, region: str, key: str, updates: Dict[str, Any], ttl: int = -1) -> Dict[str, Any]:
        """
        Alias for update_partial - Generic Update (JUPDATE) operation.
        
        Args:
            region: Cache region
            key: Document key
            updates: Dictionary with attributes to update/add
            ttl: Optional TTL for new entries
            
        Returns:
            API response
        """
        url = f"{self.base_url}/genericupdate"
        
        request_body = {
            "region": region,
            "key": key,
            "value": updates,
            "apiKey": self.api_key
        }
        
        if ttl > 0:
            request_body["ttl"] = ttl
        
        try:
            response = requests.post(url, json=request_body, headers=self.headers, timeout=30)
            result = response.json() if response.content else {}
            return {
                "status": response.status_code,
                "success": response.status_code == 200 and result.get("success", False),
                "action": result.get("action", "unknown"),
                "message": result.get("message", ""),
                "data": result
            }
        except requests.exceptions.RequestException as e:
            return {"status": 0, "success": False, "error": str(e)}
    
    # ==================== READ Operations ====================
    
    def get(self, region: str, key: str) -> Dict[str, Any]:
        """
        Get a JSON document by key.
        
        Args:
            region: Cache region
            key: Document key
            
        Returns:
            Document value or error
        """
        url = f"{self.base_url}/cache/{region}/{key}"
        
        try:
            response = requests.get(url, headers=self.headers, timeout=30)
            if response.status_code == 200:
                return {
                    "status": 200,
                    "found": True,
                    "value": response.json()
                }
            else:
                return {
                    "status": response.status_code,
                    "found": False,
                    "message": response.text
                }
        except requests.exceptions.RequestException as e:
            return {"status": 0, "found": False, "error": str(e)}
    
    # ==================== DELETE Operations ====================
    
    def delete(self, region: str, key: str) -> Dict[str, Any]:
        """
        Delete a JSON document.
        
        Args:
            region: Cache region
            key: Document key
            
        Returns:
            API response
        """
        url = f"{self.base_url}/cache/{region}/{key}"
        
        try:
            response = requests.delete(url, headers=self.headers, timeout=30)
            return {
                "status": response.status_code,
                "success": response.status_code == 200
            }
        except requests.exceptions.RequestException as e:
            return {"status": 0, "success": False, "error": str(e)}


def print_header(title: str):
    """Print a formatted section header."""
    print("\n" + "=" * 80)
    print(f"  {title}")
    print("=" * 80)


def print_subheader(title: str):
    """Print a formatted subsection header."""
    print(f"\n--- {title} ---")


def print_json(label: str, data: Any):
    """Print formatted JSON."""
    print(f"{label}: {json.dumps(data, indent=2, default=str)}")


def print_result(operation: str, result: Dict[str, Any]):
    """Print operation result."""
    if result.get("success"):
        action = result.get("action", "")
        msg = result.get("message", "Success")
        print(f"✅ {operation}: {msg}" + (f" (action: {action})" if action else ""))
    else:
        error = result.get("error") or result.get("message") or "Failed"
        print(f"❌ {operation}: {error}")


def demo_insert_operations(client: KuberDocumentClient, region: str):
    """Demonstrate INSERT operations."""
    print_header("1. INSERT OPERATIONS")
    
    # 1a. Simple insert
    print_subheader("1a. Simple Insert - New Customer")
    customer = {
        "id": "CUST001",
        "name": "John Smith",
        "email": "john.smith@email.com",
        "status": "active",
        "tier": "premium",
        "balance": 15000.50,
        "address": {
            "street": "123 Main St",
            "city": "New York",
            "state": "NY",
            "zip": "10001"
        },
        "tags": ["vip", "loyalty-member"],
        "created_at": datetime.now().isoformat()
    }
    print_json("Document", customer)
    result = client.insert(region, "customer_CUST001", customer)
    print_result("Insert", result)
    
    # Verify
    print("\nVerifying insert...")
    verify = client.get(region, "customer_CUST001")
    if verify.get("found"):
        print(f"✓ Document stored successfully")
    
    # 1b. Insert with TTL
    print_subheader("1b. Insert with TTL (60 seconds)")
    temp_data = {
        "type": "session",
        "user_id": "U123",
        "token": "abc123xyz",
        "expires_at": datetime.now().isoformat()
    }
    print_json("Document", temp_data)
    result = client.insert(region, "session_U123", temp_data, ttl=60)
    print_result("Insert with TTL=60s", result)
    
    # 1c. Bulk insert
    print_subheader("1c. Bulk Insert - Multiple Products")
    products = {
        "product_P001": {
            "sku": "P001",
            "name": "Laptop Pro",
            "category": "electronics",
            "price": 1299.99,
            "stock": 50
        },
        "product_P002": {
            "sku": "P002",
            "name": "Wireless Mouse",
            "category": "electronics",
            "price": 49.99,
            "stock": 200
        },
        "product_P003": {
            "sku": "P003",
            "name": "USB-C Cable",
            "category": "accessories",
            "price": 19.99,
            "stock": 500
        }
    }
    print(f"Inserting {len(products)} products...")
    for key, doc in products.items():
        result = client.insert(region, key, doc)
        status = "✓" if result.get("success") else "✗"
        print(f"  {status} {key}: {doc['name']}")


def demo_update_full_replace(client: KuberDocumentClient, region: str):
    """Demonstrate full document replacement."""
    print_header("2. FULL UPDATE (Replace)")
    
    # Show current state
    print_subheader("2a. Current Document State")
    current = client.get(region, "customer_CUST001")
    if current.get("found"):
        print_json("Current", current["value"])
    
    # Full replace
    print_subheader("2b. Full Replace - All Fields Changed")
    new_document = {
        "id": "CUST001",
        "name": "John Smith Jr.",  # Changed
        "email": "john.smith.jr@newemail.com",  # Changed
        "status": "inactive",  # Changed
        "tier": "basic",  # Changed
        "balance": 5000.00,  # Changed
        "phone": "+1-555-0123",  # New field
        "updated_at": datetime.now().isoformat()
        # Note: address and tags are NOT included - they will be REMOVED
    }
    print_json("New Document (replaces entire document)", new_document)
    result = client.update_full(region, "customer_CUST001", new_document)
    print_result("Full Replace", result)
    
    # Verify
    print("\nAfter full replace:")
    after = client.get(region, "customer_CUST001")
    if after.get("found"):
        print_json("Result", after["value"])
        # Check if address was removed
        if "address" not in after["value"]:
            print("⚠️  Note: 'address' field was removed (not in replacement document)")


def demo_update_partial_merge(client: KuberDocumentClient, region: str):
    """Demonstrate partial update (JUPDATE) with merge."""
    print_header("3. PARTIAL UPDATE (JUPDATE - Merge)")
    
    # First, create a fresh document
    print_subheader("3a. Setup - Create Initial Document")
    initial_customer = {
        "id": "CUST002",
        "name": "Jane Doe",
        "email": "jane@email.com",
        "status": "active",
        "tier": "standard",
        "balance": 8500.00,
        "address": {
            "street": "456 Oak Ave",
            "city": "Los Angeles",
            "state": "CA",
            "zip": "90001"
        },
        "preferences": {
            "newsletter": True,
            "sms_alerts": False
        },
        "created_at": datetime.now().isoformat()
    }
    client.insert(region, "customer_CUST002", initial_customer)
    print_json("Initial Document", initial_customer)
    
    # 3b. Partial update - single field
    print_subheader("3b. Partial Update - Change Status Only")
    updates = {
        "status": "premium"
    }
    print_json("Updates to apply", updates)
    result = client.jupdate(region, "customer_CUST002", updates)
    print_result("JUPDATE", result)
    
    after = client.get(region, "customer_CUST002")
    if after.get("found"):
        print(f"✓ status changed to: {after['value'].get('status')}")
        print(f"✓ name preserved: {after['value'].get('name')}")
        print(f"✓ email preserved: {after['value'].get('email')}")
    
    # 3c. Partial update - multiple fields
    print_subheader("3c. Partial Update - Multiple Fields")
    updates = {
        "tier": "enterprise",
        "balance": 25000.00,
        "phone": "+1-555-9999",  # New field
        "updated_at": datetime.now().isoformat()
    }
    print_json("Updates to apply", updates)
    result = client.jupdate(region, "customer_CUST002", updates)
    print_result("JUPDATE", result)
    
    after = client.get(region, "customer_CUST002")
    if after.get("found"):
        print(f"✓ tier changed to: {after['value'].get('tier')}")
        print(f"✓ balance changed to: {after['value'].get('balance')}")
        print(f"✓ phone added: {after['value'].get('phone')}")
        print(f"✓ address still exists: {'address' in after['value']}")
    
    # 3d. Partial update - nested object merge
    print_subheader("3d. Partial Update - Nested Object Merge")
    print("Updating address.city and adding address.country (preserves other address fields)")
    updates = {
        "address": {
            "city": "San Francisco",  # Change city
            "country": "USA"  # Add new field
        }
    }
    print_json("Updates to apply", updates)
    result = client.jupdate(region, "customer_CUST002", updates)
    print_result("JUPDATE (nested)", result)
    
    after = client.get(region, "customer_CUST002")
    if after.get("found"):
        addr = after['value'].get('address', {})
        print(f"✓ address.street preserved: {addr.get('street')}")
        print(f"✓ address.city changed to: {addr.get('city')}")
        print(f"✓ address.state preserved: {addr.get('state')}")
        print(f"✓ address.country added: {addr.get('country')}")
    
    # 3e. Partial update - add new nested object
    print_subheader("3e. Partial Update - Add New Nested Object")
    updates = {
        "billing": {
            "method": "credit_card",
            "last_four": "1234",
            "exp_date": "12/26"
        }
    }
    print_json("Updates to apply", updates)
    result = client.jupdate(region, "customer_CUST002", updates)
    print_result("JUPDATE", result)
    
    after = client.get(region, "customer_CUST002")
    if after.get("found"):
        print(f"✓ billing object added: {'billing' in after['value']}")
        if 'billing' in after['value']:
            print(f"  - method: {after['value']['billing'].get('method')}")


def demo_jupdate_create_vs_update(client: KuberDocumentClient, region: str):
    """Demonstrate JUPDATE behavior for new vs existing keys."""
    print_header("4. JUPDATE - CREATE vs UPDATE")
    
    # 4a. JUPDATE on non-existent key (creates new document)
    print_subheader("4a. JUPDATE on Non-Existent Key (Creates New)")
    new_key = f"order_ORD_{int(time.time())}"
    order_data = {
        "order_id": new_key,
        "customer_id": "CUST002",
        "items": [
            {"product": "P001", "qty": 1, "price": 1299.99}
        ],
        "total": 1299.99,
        "status": "pending"
    }
    print(f"Key: {new_key} (does not exist)")
    print_json("Data", order_data)
    
    result = client.jupdate(region, new_key, order_data)
    print_result("JUPDATE", result)
    print(f"Action: {result.get('action', 'unknown')}")  # Should be "created"
    
    # Verify it was created
    verify = client.get(region, new_key)
    if verify.get("found"):
        print(f"✓ Document created successfully")
    
    # 4b. JUPDATE on existing key (merges)
    print_subheader("4b. JUPDATE on Existing Key (Merges)")
    updates = {
        "status": "shipped",
        "shipped_at": datetime.now().isoformat(),
        "tracking_number": "TRK123456789"
    }
    print(f"Key: {new_key} (exists)")
    print_json("Updates", updates)
    
    result = client.jupdate(region, new_key, updates)
    print_result("JUPDATE", result)
    print(f"Action: {result.get('action', 'unknown')}")  # Should be "updated"
    
    # Verify merge
    after = client.get(region, new_key)
    if after.get("found"):
        print(f"✓ status changed to: {after['value'].get('status')}")
        print(f"✓ tracking_number added: {after['value'].get('tracking_number')}")
        print(f"✓ items preserved: {len(after['value'].get('items', []))} item(s)")
        print(f"✓ total preserved: {after['value'].get('total')}")


def demo_update_specific_attributes(client: KuberDocumentClient, region: str):
    """Demonstrate updating specific attributes by key."""
    print_header("5. UPDATE SPECIFIC ATTRIBUTES BY KEY")
    
    # Setup
    print_subheader("5a. Setup - Create Test Document")
    doc = {
        "id": "EMP001",
        "name": "Alice Johnson",
        "department": "Engineering",
        "title": "Software Developer",
        "salary": 95000,
        "skills": ["Python", "Java", "SQL"],
        "performance": {
            "rating": 4.2,
            "last_review": "2024-06-15"
        }
    }
    client.insert(region, "employee_EMP001", doc)
    print_json("Initial", doc)
    
    # 5b. Update salary only
    print_subheader("5b. Update Single Attribute - Salary")
    result = client.jupdate(region, "employee_EMP001", {"salary": 105000})
    print_result("Update salary to 105000", result)
    
    after = client.get(region, "employee_EMP001")
    print(f"New salary: {after['value'].get('salary')}")
    
    # 5c. Update title and department
    print_subheader("5c. Update Multiple Attributes - Title & Department")
    result = client.jupdate(region, "employee_EMP001", {
        "title": "Senior Software Developer",
        "department": "Platform Engineering"
    })
    print_result("Update title and department", result)
    
    after = client.get(region, "employee_EMP001")
    print(f"New title: {after['value'].get('title')}")
    print(f"New department: {after['value'].get('department')}")
    
    # 5d. Add new skills (replace array)
    print_subheader("5d. Update Array - Add New Skills")
    # Note: Arrays are replaced, not merged
    result = client.jupdate(region, "employee_EMP001", {
        "skills": ["Python", "Java", "SQL", "Kubernetes", "AWS"]
    })
    print_result("Update skills array", result)
    
    after = client.get(region, "employee_EMP001")
    print(f"New skills: {after['value'].get('skills')}")
    
    # 5e. Update nested object attributes
    print_subheader("5e. Update Nested Attributes - Performance")
    result = client.jupdate(region, "employee_EMP001", {
        "performance": {
            "rating": 4.8,
            "last_review": "2025-01-10",
            "promotion_eligible": True
        }
    })
    print_result("Update performance object", result)
    
    after = client.get(region, "employee_EMP001")
    print_json("Updated performance", after['value'].get('performance'))
    
    # 5f. Show final document
    print_subheader("5f. Final Document State")
    final = client.get(region, "employee_EMP001")
    if final.get("found"):
        print_json("Final Document", final["value"])


def main():
    """Main entry point."""
    parser = argparse.ArgumentParser(
        description="Kuber REST API Insert/Update Demo",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
    python rest_insert_update_demo.py
    python rest_insert_update_demo.py --host 192.168.1.100 --port 7070
    python rest_insert_update_demo.py --api-key kub_mykey --ssl
        """
    )
    parser.add_argument("--host", default="localhost", help="Kuber server host (default: localhost)")
    parser.add_argument("--port", type=int, default=7070, help="Kuber server port (default: 7070)")
    parser.add_argument("--api-key", default="kub_admin", help="API key (default: kub_admin)")
    parser.add_argument("--ssl", action="store_true", help="Use HTTPS")
    parser.add_argument("--region", default="update_demo", help="Region name (default: update_demo)")
    
    args = parser.parse_args()
    
    print("=" * 80)
    print("  KUBER REST API - INSERT & UPDATE DEMO (v1.7.9)")
    print("=" * 80)
    print(f"  Server:  {args.host}:{args.port}")
    print(f"  Region:  {args.region}")
    print(f"  SSL:     {'Yes' if args.ssl else 'No'}")
    print(f"  Time:    {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print("=" * 80)
    
    # Initialize client
    client = KuberDocumentClient(
        host=args.host,
        port=args.port,
        api_key=args.api_key,
        use_ssl=args.ssl
    )
    
    region = args.region
    
    try:
        # Run all demonstrations
        demo_insert_operations(client, region)
        demo_update_full_replace(client, region)
        demo_update_partial_merge(client, region)
        demo_jupdate_create_vs_update(client, region)
        demo_update_specific_attributes(client, region)
        
        # Summary
        print_header("DEMO COMPLETE")
        print("""
This demo showcased Insert & Update operations:

  1. INSERT OPERATIONS
     - Simple insert: POST /api/v1/cache/{region}/{key}
     - Insert with TTL: POST /api/v1/cache/{region}/{key}?ttl=60
     - Bulk insert: POST /api/v1/cache/{region}/batch

  2. FULL UPDATE (Replace)
     - Same as insert - completely replaces the document
     - Fields not included are REMOVED

  3. PARTIAL UPDATE (JUPDATE - Merge)
     - POST /api/v1/genericupdate
     - Only updates specified fields
     - Existing fields are PRESERVED
     - Nested objects are DEEP MERGED
     - Arrays are REPLACED (not merged)

  4. JUPDATE CREATE vs UPDATE
     - Non-existent key: Creates new document
     - Existing key: Merges with existing document

  5. UPDATE SPECIFIC ATTRIBUTES
     - Pass only the fields you want to change
     - All other fields remain unchanged

Key Endpoints:
    POST /api/v1/cache/{region}/{key}     - Insert/Replace
    POST /api/v1/genericupdate            - JUPDATE (Create or Merge)
    GET  /api/v1/cache/{region}/{key}     - Read
    DELETE /api/v1/cache/{region}/{key}   - Delete

Request Body for JUPDATE:
    {
        "region": "myregion",
        "key": "mykey",
        "value": {
            "field1": "new_value",
            "field2": 123
        },
        "apiKey": "kub_xxx"
    }
        """)
        
    except KeyboardInterrupt:
        print("\n\nDemo interrupted by user.")
    except Exception as e:
        print(f"\n❌ Error: {e}")
        raise


if __name__ == "__main__":
    main()
