#!/usr/bin/env python3
"""
Kuber Comprehensive Cache Operations via Kafka (v2.6.0)

Demonstrates every cache operation through Kafka request/response messaging:
  - String operations:  SET, GET, DELETE, EXISTS, KEYS, TTL, EXPIRE, MGET, MSET
  - JSON operations:    JSET, JGET, JUPDATE, JREMOVE, JSEARCH
  - Hash operations:    HSET, HGET, HGETALL
  - Admin operations:   PING, INFO, REGIONS

Prerequisites:
  - pip install kafka-python
  - Kafka running (default: localhost:9092)
  - Kuber server running with request/response messaging enabled
  - Valid API key from Kuber Admin → API Keys

Copyright © 2025-2030 Ashutosh Sinha. All Rights Reserved.
"""

import json
import uuid
import time
import threading
from kafka import KafkaProducer, KafkaConsumer

# ── Configuration ──────────────────────────────────────────────────────────────
BOOTSTRAP_SERVERS = "localhost:9092"
REQUEST_TOPIC     = "kuber_cache_request"
RESPONSE_TOPIC    = "kuber_cache_response"
API_KEY           = "YOUR_API_KEY"     # from /admin/apikeys
REGION            = "employees"         # target cache region
TIMEOUT_SECONDS   = 10                  # response wait timeout

# ── Internal state ─────────────────────────────────────────────────────────────
pending = {}          # message_id -> threading.Event, response holder
pending_lock = threading.Lock()
running = True


# ══════════════════════════════════════════════════════════════════════════════
#  KAFKA INFRASTRUCTURE
# ══════════════════════════════════════════════════════════════════════════════

def create_producer():
    return KafkaProducer(
        bootstrap_servers=BOOTSTRAP_SERVERS,
        key_serializer=lambda k: k.encode("utf-8") if k else None,
        value_serializer=lambda v: v.encode("utf-8"),
        acks="all",
    )


def create_consumer():
    return KafkaConsumer(
        RESPONSE_TOPIC,
        bootstrap_servers=BOOTSTRAP_SERVERS,
        group_id=f"kuber-python-demo-{uuid.uuid4()}",
        auto_offset_reset="latest",
        enable_auto_commit=True,
        value_deserializer=lambda m: m.decode("utf-8"),
        consumer_timeout_ms=200,
    )


def response_listener(consumer_instance):
    """Background thread polling Kafka for responses and completing pending futures."""
    global running
    while running:
        try:
            records = consumer_instance.poll(timeout_ms=100)
            for tp, messages in records.items():
                for msg in messages:
                    try:
                        resp = json.loads(msg.value)
                        # Correlate via message_id
                        message_id = None
                        if "request" in resp and "message_id" in resp["request"]:
                            message_id = resp["request"]["message_id"]
                        elif "message_id" in resp:
                            message_id = resp["message_id"]

                        if message_id:
                            with pending_lock:
                                if message_id in pending:
                                    pending[message_id]["response"] = resp
                                    pending[message_id]["event"].set()
                    except Exception as e:
                        print(f"  Error parsing response: {e}")
        except Exception:
            pass
    consumer_instance.close()


def send_and_wait(producer_instance, operation, region=None, key=None, **kwargs):
    """Build request JSON, publish to Kafka, and wait for correlated response."""
    message_id = str(uuid.uuid4())

    request = {
        "api_key": API_KEY,
        "message_id": message_id,
        "operation": operation.upper(),
    }
    if region:
        request["region"] = region
    if key:
        request["key"] = key
    # Merge extra fields (value, ttl, pattern, keys, entries, field, path, query, etc.)
    request.update(kwargs)

    # Register pending
    event = threading.Event()
    holder = {"event": event, "response": None}
    with pending_lock:
        pending[message_id] = holder

    # Publish
    payload = json.dumps(request)
    producer_instance.send(REQUEST_TOPIC, key=message_id.encode("utf-8"), value=payload.encode("utf-8"))
    producer_instance.flush()

    # Wait for response
    if event.wait(timeout=TIMEOUT_SECONDS):
        with pending_lock:
            pending.pop(message_id, None)
        return holder["response"]
    else:
        with pending_lock:
            pending.pop(message_id, None)
        raise TimeoutError(f"Timeout waiting for {operation} (message_id={message_id})")


# ══════════════════════════════════════════════════════════════════════════════
#  OUTPUT HELPERS
# ══════════════════════════════════════════════════════════════════════════════

def print_response(resp):
    success = resp.get("success", False)
    status = "✅ SUCCESS" if success else "❌ FAIL"
    print(f"   Status: {status}")
    if "result" in resp:
        pretty = json.dumps(resp["result"], indent=4)
        for line in pretty.split("\n"):
            print(f"   {line}")
    if "error" in resp:
        print(f"   Error: {resp['error']}")
    print()


def banner(title):
    print(f"\n{'═' * 70}")
    print(f"  {title}")
    print(f"{'═' * 70}\n")


def section(title):
    print(f"── {title} ──")


# ══════════════════════════════════════════════════════════════════════════════
#  DEMO FUNCTIONS
# ══════════════════════════════════════════════════════════════════════════════

def demo_admin(p):
    banner("ADMIN OPERATIONS")

    section("PING - Health Check")
    print_response(send_and_wait(p, "PING"))

    section("INFO - Server Information")
    print_response(send_and_wait(p, "INFO"))

    section("REGIONS - List All Regions")
    print_response(send_and_wait(p, "REGIONS"))


def demo_string_ops(p):
    banner("STRING OPERATIONS (SET / GET / DELETE / EXISTS / KEYS / TTL / EXPIRE)")

    section("SET - Store string value")
    print_response(send_and_wait(p, "SET", REGION, "emp:1001",
                                  value="John Doe - Engineering Manager", ttl=3600))

    section("GET - Retrieve value")
    print_response(send_and_wait(p, "GET", REGION, "emp:1001"))

    section("EXISTS - Check key exists")
    print_response(send_and_wait(p, "EXISTS", REGION, "emp:1001"))

    section("TTL - Check remaining TTL")
    print_response(send_and_wait(p, "TTL", REGION, "emp:1001"))

    section("EXPIRE - Set new TTL (7200s)")
    print_response(send_and_wait(p, "EXPIRE", REGION, "emp:1001", ttl=7200))

    section("KEYS - Find keys matching 'emp:*'")
    print_response(send_and_wait(p, "KEYS", REGION, pattern="emp:*"))

    section("DELETE - Remove key")
    print_response(send_and_wait(p, "DELETE", REGION, "emp:1001"))

    section("GET - Verify deletion (expect null)")
    print_response(send_and_wait(p, "GET", REGION, "emp:1001"))


def demo_batch_ops(p):
    banner("BATCH OPERATIONS (MSET / MGET)")

    section("MSET - Batch store 3 entries")
    print_response(send_and_wait(p, "MSET", REGION, entries={
        "dept:engineering": "Software Engineering",
        "dept:marketing": "Digital Marketing",
        "dept:finance": "Corporate Finance",
    }))

    section("MGET - Batch retrieve 3 entries (+ 1 nonexistent)")
    print_response(send_and_wait(p, "MGET", REGION,
                                  keys=["dept:engineering", "dept:marketing",
                                        "dept:finance", "dept:nonexistent"]))


def demo_json_ops(p):
    banner("JSON OPERATIONS (JSET / JGET / JUPDATE / JREMOVE / JSEARCH)")

    section("JSET - Store employee JSON document (E001)")
    print_response(send_and_wait(p, "JSET", REGION, "employee/E001", value={
        "name": "Jane Smith",
        "email": "jane.smith@acme.com",
        "department": "Engineering",
        "title": "Senior Software Engineer",
        "salary": 145000,
        "active": True,
        "address": {"city": "San Francisco", "state": "CA", "zip": "94105"},
        "skills": ["Java", "Kafka", "Kubernetes", "PostgreSQL"],
    }))

    section("JSET - Store second employee (E002)")
    print_response(send_and_wait(p, "JSET", REGION, "employee/E002", value={
        "name": "Bob Johnson",
        "email": "bob.johnson@acme.com",
        "department": "Engineering",
        "title": "DevOps Engineer",
        "salary": 135000,
        "active": True,
        "address": {"city": "Seattle", "state": "WA", "zip": "98101"},
        "skills": ["Docker", "Kafka", "Terraform", "AWS"],
    }))

    section("JSET - Store marketing employee (E003)")
    print_response(send_and_wait(p, "JSET", REGION, "employee/E003", value={
        "name": "Alice Chen",
        "email": "alice.chen@acme.com",
        "department": "Marketing",
        "title": "Marketing Director",
        "salary": 160000,
        "active": True,
        "address": {"city": "New York", "state": "NY", "zip": "10001"},
        "skills": ["Analytics", "SEO", "Content Strategy"],
    }))

    section("JGET - Retrieve full employee document (E001)")
    print_response(send_and_wait(p, "JGET", REGION, "employee/E001"))

    section("JGET - Retrieve only the address (JSONPath: $.address)")
    print_response(send_and_wait(p, "JGET", REGION, "employee/E001", path="$.address"))

    section("JUPDATE - Promote employee (update title + salary, add field)")
    print_response(send_and_wait(p, "JUPDATE", REGION, "employee/E001", value={
        "title": "Staff Software Engineer",
        "salary": 175000,
        "promotion_date": "2026-02-16",
    }))

    section("JGET - Verify updated employee")
    print_response(send_and_wait(p, "JGET", REGION, "employee/E001"))

    section("JREMOVE - Remove 'promotion_date' attribute")
    print_response(send_and_wait(p, "JREMOVE", REGION, "employee/E001",
                                  keys=["promotion_date"]))

    section("JSEARCH - Find all Engineering department employees")
    print_response(send_and_wait(p, "JSEARCH", REGION,
                                  query="$.department == 'Engineering'"))

    section("JSEARCH - Find employees in California")
    print_response(send_and_wait(p, "JSEARCH", REGION,
                                  query="$.address.state == 'CA'"))

    section("JSEARCH - Find employees with salary > 150000")
    print_response(send_and_wait(p, "JSEARCH", REGION,
                                  query="$.salary > 150000"))


def demo_hash_ops(p):
    banner("HASH OPERATIONS (HSET / HGET / HGETALL)")

    section("HSET - Set hash fields for config:app")
    for field, value in [
        ("version", "2.6.0"),
        ("max_connections", "500"),
        ("log_level", "INFO"),
        ("feature_flags", '{"dark_mode":true,"beta_api":false}'),
    ]:
        resp = send_and_wait(p, "HSET", REGION, "config:app", field=field, value=value)
        ok = resp.get("success", False)
        print(f"   Set field '{field}': {'OK' if ok else 'FAIL'}")
    print()

    section("HGET - Get 'version' field from config:app")
    print_response(send_and_wait(p, "HGET", REGION, "config:app", field="version"))

    section("HGETALL - Get all fields from config:app")
    print_response(send_and_wait(p, "HGETALL", REGION, "config:app"))


def demo_cleanup(p):
    banner("CLEANUP")
    section("DELETE - Clean up all demo keys")
    for key in ["emp:1001", "dept:engineering", "dept:marketing", "dept:finance",
                "employee/E001", "employee/E002", "employee/E003", "config:app"]:
        send_and_wait(p, "DELETE", REGION, key)
        print(f"   Deleted: {key}")
    print()


# ══════════════════════════════════════════════════════════════════════════════
#  MAIN
# ══════════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    print("╔═══════════════════════════════════════════════════════════════════════╗")
    print("║  Kuber Cache Operations via Kafka — Comprehensive Demo (v2.6.0)       ║")
    print(f"║  Region:   {REGION:<57}  ║")
    print(f"║  Request:  {REQUEST_TOPIC:<55}  ║")
    print(f"║  Response: {RESPONSE_TOPIC:<55}  ║")
    print("╚═══════════════════════════════════════════════════════════════════════╝\n")

    producer = create_producer()
    consumer = create_consumer()

    # Start response listener in background
    listener_thread = threading.Thread(target=response_listener, args=(consumer,), daemon=True)
    listener_thread.start()
    time.sleep(2)
    print(f"✅ Connected to Kafka. Consumer subscribed to: {RESPONSE_TOPIC}\n")

    try:
        demo_admin(producer)
        demo_string_ops(producer)
        demo_batch_ops(producer)
        demo_json_ops(producer)
        demo_hash_ops(producer)
        demo_cleanup(producer)
    finally:
        running = False
        producer.close(timeout=5)
        listener_thread.join(timeout=3)
        print("✅ Kafka clients shut down.")
