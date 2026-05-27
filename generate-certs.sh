#!/bin/bash
set -euo pipefail

CERTS_DIR="$(cd "$(dirname "$0")" && pwd)/certs"
PASSWORD="changeit"
VALIDITY=365
ALIAS="kafka-broker"
DNAME="CN=kafka-broker, OU=Dev, O=Local, L=Local, ST=Local, C=AU"

mkdir -p "$CERTS_DIR"

echo "=== Step 1: Generate self-signed broker keystore ==="
keytool -genkeypair \
  -alias "$ALIAS" \
  -keyalg RSA \
  -keysize 2048 \
  -validity "$VALIDITY" \
  -dname "$DNAME" \
  -keystore "$CERTS_DIR/kafka.broker.keystore.jks" \
  -storepass "$PASSWORD" \
  -keypass "$PASSWORD" \
  -ext "SAN=dns:localhost,dns:kafka-broker,dns:kafka-sasl"

echo "=== Step 2: Export certificate to PEM ==="
keytool -exportcert \
  -alias "$ALIAS" \
  -keystore "$CERTS_DIR/kafka.broker.keystore.jks" \
  -storepass "$PASSWORD" \
  -file "$CERTS_DIR/broker-cert.pem" \
  -rfc

echo "=== Step 3: Create truststore with broker certificate ==="
keytool -importcert \
  -alias "$ALIAS" \
  -keystore "$CERTS_DIR/kafka.client.truststore.jks" \
  -storepass "$PASSWORD" \
  -file "$CERTS_DIR/broker-cert.pem" \
  -noprompt

echo ""
echo "Certificates generated in: $CERTS_DIR"
echo "Keystore password: $PASSWORD"
ls -la "$CERTS_DIR"
