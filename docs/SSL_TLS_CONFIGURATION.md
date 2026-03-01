# Kuber SSL/TLS Configuration Guide

## Overview

Kuber supports SSL/TLS encryption for both its communication channels:

| Channel | Description | Default Port |
|---------|-------------|--------------|
| **HTTPS** | REST API & Web UI | 8443 |
| **Redis Protocol SSL** | KuberClient connections | 6381 |

This guide covers:
- Server SSL configuration
- Client SSL configuration (Java, Python, C#)
- REST API client SSL setup
- Mutual TLS (mTLS)
- Certificate management
- Troubleshooting

---

## Part A: Server SSL Configuration

### 1. Certificate Setup

#### Option 1: Self-Signed Certificate (Development/Testing)

```bash
# Generate a new keystore with self-signed certificate
keytool -genkeypair \
    -alias kuber \
    -keyalg RSA \
    -keysize 2048 \
    -validity 365 \
    -keystore kuber-keystore.p12 \
    -storetype PKCS12 \
    -storepass changeit \
    -keypass changeit \
    -dname "CN=localhost, OU=Kuber, O=MyCompany, L=City, ST=State, C=US"

# Verify the keystore
keytool -list -keystore kuber-keystore.p12 -storetype PKCS12 -storepass changeit
```

#### Option 2: CA-Signed Certificate (Production)

```bash
# Step 1: Generate a key pair and CSR
keytool -genkeypair \
    -alias kuber \
    -keyalg RSA \
    -keysize 2048 \
    -keystore kuber-keystore.p12 \
    -storetype PKCS12 \
    -storepass YourSecurePassword \
    -dname "CN=kuber.yourcompany.com, OU=IT, O=YourCompany, L=City, ST=State, C=US"

# Step 2: Generate CSR
keytool -certreq \
    -alias kuber \
    -keystore kuber-keystore.p12 \
    -storetype PKCS12 \
    -storepass YourSecurePassword \
    -file kuber.csr

# Step 3: Submit CSR to your CA and receive signed certificate

# Step 4: Import CA certificate chain
keytool -importcert \
    -alias root-ca \
    -keystore kuber-keystore.p12 \
    -storetype PKCS12 \
    -storepass YourSecurePassword \
    -file root-ca.crt \
    -trustcacerts

# Step 5: Import signed certificate
keytool -importcert \
    -alias kuber \
    -keystore kuber-keystore.p12 \
    -storetype PKCS12 \
    -storepass YourSecurePassword \
    -file kuber-signed.crt
```

#### Option 3: Convert Existing PEM Files

```bash
# Convert PEM to PKCS12
openssl pkcs12 -export \
    -in certificate.pem \
    -inkey private-key.pem \
    -out kuber-keystore.p12 \
    -name kuber \
    -password pass:YourSecurePassword

# Include CA chain if available
openssl pkcs12 -export \
    -in certificate.pem \
    -inkey private-key.pem \
    -certfile ca-chain.pem \
    -out kuber-keystore.p12 \
    -name kuber \
    -password pass:YourSecurePassword
```

### 2. HTTPS Configuration (REST API & Web UI)

Add to `application.properties`:

```properties
# =============================================================================
# SSL/TLS Configuration for HTTPS (REST API & Web UI)
# =============================================================================

# Enable HTTPS
server.port=8443
server.ssl.enabled=true

# Keystore configuration
server.ssl.key-store=file:/opt/kuber/config/kuber-keystore.p12
server.ssl.key-store-password=YourSecurePassword
server.ssl.key-store-type=PKCS12
server.ssl.key-alias=kuber

# TLS Protocol version (recommended: TLSv1.2 or TLSv1.3)
server.ssl.protocol=TLS
server.ssl.enabled-protocols=TLSv1.2,TLSv1.3

# Cipher suites (strong ciphers only)
server.ssl.ciphers=TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,TLS_DHE_RSA_WITH_AES_256_GCM_SHA384,TLS_DHE_RSA_WITH_AES_128_GCM_SHA256
```

#### Environment Variable Override

```bash
# Docker / Kubernetes environment variables
export SERVER_PORT=8443
export SERVER_SSL_ENABLED=true
export SERVER_SSL_KEY_STORE=file:/opt/kuber/config/kuber-keystore.p12
export SERVER_SSL_KEY_STORE_PASSWORD=YourSecurePassword
export SERVER_SSL_KEY_STORE_TYPE=PKCS12
export SERVER_SSL_KEY_ALIAS=kuber
export SERVER_SSL_ENABLED_PROTOCOLS=TLSv1.2,TLSv1.3
```

#### Command Line Override

```bash
java -jar kuber-server.jar \
    --server.port=8443 \
    --server.ssl.enabled=true \
    --server.ssl.key-store=file:./kuber-keystore.p12 \
    --server.ssl.key-store-password=YourSecurePassword \
    --server.ssl.key-store-type=PKCS12
```

### 3. Redis Protocol SSL Configuration

Add to `application.properties`:

```properties
# =============================================================================
# SSL/TLS Configuration for Redis Protocol
# =============================================================================

# Redis protocol port (non-SSL)
kuber.network.port=6380

# Enable SSL for Redis protocol
kuber.network.ssl.enabled=true
kuber.network.ssl.port=6381

# SSL keystore (can use same as HTTPS or separate)
kuber.network.ssl.key-store=file:/opt/kuber/config/kuber-keystore.p12
kuber.network.ssl.key-store-password=YourSecurePassword
kuber.network.ssl.key-store-type=PKCS12

# TLS protocol settings
kuber.network.ssl.protocol=TLSv1.2
kuber.network.ssl.ciphers=TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256

# For Mutual TLS (client certificate validation)
# kuber.network.ssl.client-auth=REQUIRE
# kuber.network.ssl.trust-store=file:/opt/kuber/config/kuber-truststore.p12
# kuber.network.ssl.trust-store-password=TrustStorePassword
```

#### Test with Redis CLI

```bash
# Connect using redis-cli with TLS
redis-cli -h localhost -p 6381 --tls \
    --cert /path/to/client.crt \
    --key /path/to/client.key \
    --cacert /path/to/ca.crt

# Skip certificate verification (development only!)
redis-cli -h localhost -p 6381 --tls --insecure
```

---

## Part B: Client SSL Configuration

### 1. Java Client (KuberClient)

#### Option 1: Using System Properties

```java
// Set SSL system properties before creating client
System.setProperty("javax.net.ssl.trustStore", "/path/to/truststore.p12");
System.setProperty("javax.net.ssl.trustStorePassword", "TrustStorePassword");
System.setProperty("javax.net.ssl.trustStoreType", "PKCS12");

// For mutual TLS (client certificate)
System.setProperty("javax.net.ssl.keyStore", "/path/to/client-keystore.p12");
System.setProperty("javax.net.ssl.keyStorePassword", "KeyStorePassword");
System.setProperty("javax.net.ssl.keyStoreType", "PKCS12");

// Create client with SSL port
try (KuberClient client = new KuberClient("kuber.example.com", 6381, "kub_your_api_key")) {
    client.set("key", "value");
    String value = client.get("key");
}
```

#### Option 2: Programmatic SSLContext

```java
import javax.net.ssl.*;
import java.security.KeyStore;
import java.io.FileInputStream;

public class SecureKuberClient {
    
    public static SSLSocketFactory createSSLSocketFactory() throws Exception {
        // Load truststore
        KeyStore trustStore = KeyStore.getInstance("PKCS12");
        try (FileInputStream fis = new FileInputStream("/path/to/truststore.p12")) {
            trustStore.load(fis, "TrustStorePassword".toCharArray());
        }
        
        TrustManagerFactory tmf = TrustManagerFactory.getInstance(
            TrustManagerFactory.getDefaultAlgorithm());
        tmf.init(trustStore);
        
        // Optional: Load keystore for client certificate (mTLS)
        KeyStore keyStore = KeyStore.getInstance("PKCS12");
        try (FileInputStream fis = new FileInputStream("/path/to/client-keystore.p12")) {
            keyStore.load(fis, "KeyStorePassword".toCharArray());
        }
        
        KeyManagerFactory kmf = KeyManagerFactory.getInstance(
            KeyManagerFactory.getDefaultAlgorithm());
        kmf.init(keyStore, "KeyStorePassword".toCharArray());
        
        // Create SSLContext
        SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
        sslContext.init(kmf.getKeyManagers(), tmf.getTrustManagers(), null);
        
        return sslContext.getSocketFactory();
    }
}
```

### 2. Python Client

#### Redis Protocol with SSL

```python
import ssl
from kuber import KuberClient

# Create SSL context with certificate verification
ssl_context = ssl.create_default_context(ssl.Purpose.SERVER_AUTH)
ssl_context.load_verify_locations('/path/to/ca-certificate.pem')

# For mutual TLS (client certificate)
ssl_context.load_cert_chain(
    certfile='/path/to/client-certificate.pem',
    keyfile='/path/to/client-private-key.pem',
    password='key_password'  # Optional if key is encrypted
)

# Create client with SSL
with KuberClient('kuber.example.com', 6381, 'kub_your_api_key', ssl_context=ssl_context) as client:
    client.set('key', 'value')
    value = client.get('key')
    print(f"Retrieved: {value}")
```

#### REST API with SSL (requests library)

```python
import requests

# Verify server certificate with CA bundle
response = requests.post(
    'https://kuber.example.com:8443/api/genericsearch',
    json={
        'apiKey': 'kub_your_api_key',
        'region': 'users',
        'keyPattern': 'user:.*'
    },
    verify='/path/to/ca-certificate.pem'
)

# With client certificate (mTLS)
response = requests.post(
    'https://kuber.example.com:8443/api/genericsearch',
    json={
        'apiKey': 'kub_your_api_key',
        'region': 'users',
        'key': 'user:123'
    },
    cert=('/path/to/client-cert.pem', '/path/to/client-key.pem'),
    verify='/path/to/ca-certificate.pem'
)

print(response.json())
```

### 3. C# Client

#### Using StackExchange.Redis with SSL

```csharp
using StackExchange.Redis;
using System.Security.Cryptography.X509Certificates;
using System.Net.Security;

var options = new ConfigurationOptions
{
    EndPoints = { { "kuber.example.com", 6381 } },
    Ssl = true,
    SslProtocols = System.Security.Authentication.SslProtocols.Tls12,
    Password = "your_password",
    AbortOnConnectFail = false
};

// Custom certificate validation
options.CertificateValidation += (sender, certificate, chain, sslPolicyErrors) =>
{
    if (sslPolicyErrors == SslPolicyErrors.None)
        return true;
    
    // Add custom validation logic here
    return false;
};

// With client certificate (mTLS)
options.CertificateSelection += (sender, targetHost, localCertificates, 
                                  remoteCertificate, acceptableIssuers) =>
{
    var clientCert = new X509Certificate2(
        "/path/to/client-certificate.pfx", 
        "certificate_password"
    );
    return clientCert;
};

var redis = ConnectionMultiplexer.Connect(options);
var db = redis.GetDatabase();
```

#### REST API with HttpClient

```csharp
using System.Net.Http;
using System.Security.Cryptography.X509Certificates;

var handler = new HttpClientHandler();

// Trust custom CA certificate
handler.ServerCertificateCustomValidationCallback = (message, cert, chain, errors) =>
{
    var caCert = new X509Certificate2("/path/to/ca-certificate.crt");
    chain.ChainPolicy.ExtraStore.Add(caCert);
    return chain.Build(new X509Certificate2(cert));
};

// With client certificate (mTLS)
var clientCert = new X509Certificate2("/path/to/client.pfx", "password");
handler.ClientCertificates.Add(clientCert);

var httpClient = new HttpClient(handler);
var response = await httpClient.PostAsync(
    "https://kuber.example.com:8443/api/genericsearch",
    new StringContent(
        JsonSerializer.Serialize(new { apiKey = "kub_xxx", region = "users", key = "user:123" }),
        Encoding.UTF8,
        "application/json"
    )
);
```

### 4. REST Clients (cURL, Postman)

#### cURL Examples

```bash
# Basic HTTPS with CA certificate
curl -X POST https://kuber.example.com:8443/api/genericsearch \
    --cacert /path/to/ca-certificate.pem \
    -H "Content-Type: application/json" \
    -d '{"apiKey": "kub_xxx", "region": "users", "key": "user:123"}'

# With client certificate (mTLS)
curl -X POST https://kuber.example.com:8443/api/genericsearch \
    --cacert /path/to/ca-certificate.pem \
    --cert /path/to/client-cert.pem \
    --key /path/to/client-key.pem \
    -H "Content-Type: application/json" \
    -d '{"apiKey": "kub_xxx", "region": "products", "keyPattern": "prod:.*"}'

# Skip verification (development only!)
curl -k https://kuber.example.com:8443/api/genericsearch \
    -H "Content-Type: application/json" \
    -d '{"apiKey": "kub_xxx", "region": "test", "key": "test:1"}'
```

#### Postman Configuration

1. Go to **Settings → Certificates**
2. Add CA Certificate:
   - Host: `kuber.example.com`
   - Port: `8443`
   - CA Certificate: Select your CA cert file
3. For mTLS, also add:
   - Client Certificate (CRT file)
   - Client Key (KEY file)

---

## Mutual TLS (mTLS)

Mutual TLS requires both server and client to present certificates.

### Server Configuration

```properties
# application.properties
server.ssl.client-auth=NEED

# Trust store with allowed client CAs
server.ssl.trust-store=file:/path/to/truststore.p12
server.ssl.trust-store-password=TrustStorePassword
server.ssl.trust-store-type=PKCS12

# For Redis protocol
kuber.network.ssl.client-auth=REQUIRE
kuber.network.ssl.trust-store=file:/path/to/truststore.p12
kuber.network.ssl.trust-store-password=TrustStorePassword
```

### Generate Client Certificate

```bash
# Generate client key and CSR
openssl req -new -newkey rsa:2048 \
    -keyout client-key.pem \
    -out client.csr \
    -nodes \
    -subj "/CN=client-app/O=MyCompany"

# Sign with your CA
openssl x509 -req \
    -in client.csr \
    -CA ca-cert.pem \
    -CAkey ca-key.pem \
    -CAcreateserial \
    -out client-cert.pem \
    -days 365

# Convert to PKCS12 (for Java/.NET)
openssl pkcs12 -export \
    -in client-cert.pem \
    -inkey client-key.pem \
    -out client.p12
```

---

## Troubleshooting

| Error | Cause | Solution |
|-------|-------|----------|
| `PKIX path building failed` | Server certificate not trusted | Add CA certificate to client truststore |
| `unable to find valid certification path` | Missing CA in trust chain | Import intermediate and root CA certificates |
| `Certificate expired` | Certificate validity ended | Renew and deploy new certificate |
| `Hostname verification failed` | CN/SAN doesn't match hostname | Generate certificate with correct CN or SAN |
| `SSLHandshakeException` | Protocol/cipher mismatch | Ensure client supports server's TLS version |
| `Connection reset` | Client certificate required | Configure client certificate for mTLS |

### Debugging Commands

```bash
# Test SSL connection
openssl s_client -connect kuber.example.com:8443 -showcerts

# Check certificate details
openssl x509 -in certificate.pem -text -noout

# Verify certificate chain
openssl verify -CAfile ca-cert.pem server-cert.pem

# List keystore contents
keytool -list -v -keystore kuber-keystore.p12 -storetype PKCS12

# Enable Java SSL debugging
java -Djavax.net.debug=ssl:handshake -jar kuber-server.jar
```

---

## Best Practices

### Do ✓

- Use TLS 1.2 or 1.3 (disable older versions)
- Use strong cipher suites (AES-GCM)
- Use CA-signed certificates in production
- Set certificate expiry alerts
- Rotate certificates before expiration
- Use separate certificates per environment
- Secure keystore passwords using secrets management
- Enable hostname verification
- Use 2048-bit or 4096-bit RSA keys (or ECDSA)

### Don't ✗

- Use self-signed certificates in production
- Disable certificate verification
- Use SSLv3, TLS 1.0, or TLS 1.1
- Hard-code passwords in source code
- Use weak ciphers (RC4, DES, 3DES)
- Share private keys between services
- Ignore certificate expiration warnings
- Store private keys in version control

---

## Quick Reference

| Configuration | Property | Default |
|--------------|----------|---------|
| HTTPS Port | `server.port` | 8080 (8443 with SSL) |
| Enable HTTPS | `server.ssl.enabled` | false |
| Keystore Path | `server.ssl.key-store` | (required) |
| Redis SSL Port | `kuber.network.ssl.port` | 6381 |
| TLS Protocols | `server.ssl.enabled-protocols` | TLSv1.2,TLSv1.3 |
| Client Auth (mTLS) | `server.ssl.client-auth` | none |

---

## Part D: Message Broker SSL/TLS

Kuber's message brokers (Kafka, Confluent Kafka, RabbitMQ, ActiveMQ, IBM MQ) each support SSL/TLS for encrypted communication. Broker SSL is configured in `config/message_brokers.json`.

### Supported SSL Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| `jks` | Server-only trust via JKS truststore | Kafka with one-way TLS |
| `pem` | Server-only trust via PEM CA cert | RabbitMQ, general purpose |
| `sasl_ssl` | SASL authentication over SSL (SCRAM, PLAIN) | Kafka with SASL |
| `mtls_jks` | Mutual TLS via JKS keystore + truststore | Kafka, ActiveMQ, IBM MQ mTLS |
| `mtls_pem` | Mutual TLS via PEM certs + key | RabbitMQ, Kafka mTLS |

### Confluent Kafka (confluent-kafka)

Confluent Kafka uses built-in SASL_SSL with API key/secret authentication — no manual SSL or JAAS configuration is needed. Simply provide `api-key` and `api-secret` in the broker definition:

```json
{
  "confluent-cloud": {
    "enabled": true,
    "type": "confluent-kafka",
    "bootstrap-servers": "pkc-xxxxx.us-east-1.aws.confluent.cloud:9092",
    "api-key": "YOUR_CONFLUENT_API_KEY",
    "api-secret": "YOUR_CONFLUENT_API_SECRET",
    "partitions": 6,
    "replication-factor": 3,
    "retention-hours": 168,
    "acks": "all"
  }
}
```

The adapter automatically constructs the SASL_SSL JAAS config, sets `security.protocol=SASL_SSL` and `sasl.mechanism=PLAIN`. No truststore or keystore paths are required for Confluent Cloud.

### Kafka SSL Examples

**One-Way TLS (JKS):**
```json
{
  "ssl": {
    "enabled": true,
    "mode": "jks",
    "protocol": "TLSv1.3",
    "trust-store-path": "/opt/kuber/certs/kafka-truststore.jks",
    "trust-store-password": "changeit",
    "trust-store-type": "JKS"
  }
}
```

**SASL_SSL (SCRAM-SHA-512):**
```json
{
  "ssl": {
    "enabled": true,
    "mode": "sasl_ssl",
    "protocol": "TLSv1.3",
    "sasl-mechanism": "SCRAM-SHA-512",
    "sasl-jaas-config": "org.apache.kafka.common.security.scram.ScramLoginModule required username=\"user\" password=\"pass\";",
    "trust-store-path": "/opt/kuber/certs/truststore.jks",
    "trust-store-password": "changeit"
  }
}
```

**Mutual TLS (mTLS with JKS):**
```json
{
  "ssl": {
    "enabled": true,
    "mode": "mtls_jks",
    "protocol": "TLSv1.3",
    "trust-store-path": "/opt/kuber/certs/truststore.jks",
    "trust-store-password": "changeit",
    "key-store-path": "/opt/kuber/certs/client-keystore.jks",
    "key-store-password": "clientpass"
  }
}
```

**Mutual TLS (mTLS with PEM):**
```json
{
  "ssl": {
    "enabled": true,
    "mode": "mtls_pem",
    "protocol": "TLSv1.3",
    "trust-cert-path": "/opt/kuber/certs/ca-cert.pem",
    "key-cert-path": "/opt/kuber/certs/client-cert.pem",
    "key-path": "/opt/kuber/certs/client-key.pem"
  }
}
```

---

## See Also

- [Security & RBAC](SECURITY.md)
- [API Keys](docs/API_KEYS.md)
- [Client Libraries](CLIENT_USAGE.md)
- [Application Properties](APPLICATION_PROPERTIES.md)
