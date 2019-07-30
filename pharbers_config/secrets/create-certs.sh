#!/bin/bash

set -o nounset \
    -o errexit \
    -o verbose \
    -o xtrace

# Generate CA key
openssl req -new -x509 -keyout snakeoil-ca-1.key -out snakeoil-ca-1.crt -days 365 -subj '/CN=ca1.test.pharbers.io/OU=TEST/O=PHARBERS/L=PaloAlto/S=Ca/C=US' -passin pass:pharbers -passout pass:pharbers

# Kafkacat
openssl genrsa -des3 -passout "pass:pharbers" -out kafkacat.client.key 1024
openssl req -passin "pass:pharbers" -passout "pass:pharbers" -key kafkacat.client.key -new -out kafkacat.client.req -subj '/CN=kafkacat.test.pharbers.io/OU=TEST/O=PHARBERS/L=PaloAlto/S=Ca/C=US'
openssl x509 -req -CA snakeoil-ca-1.crt -CAkey snakeoil-ca-1.key -in kafkacat.client.req -out kafkacat-ca1-signed.pem -days 9999 -CAcreateserial -passin "pass:pharbers"



for i in broker1 broker2 producer consumer
do
	echo $i
	# Create keystores
	keytool -genkey -noprompt \
				 -alias $i \
				 -dname "CN=$i.test.pharbers.io, OU=TEST, O=PHARBERS, L=PaloAlto, S=Ca, C=US" \
				 -keystore kafka.$i.keystore.jks \
				 -keyalg RSA \
				 -storepass pharbers \
				 -keypass pharbers

	# Create CSR, sign the key and import back into keystore
	keytool -keystore kafka.$i.keystore.jks -alias $i -certreq -file $i.csr -storepass pharbers -keypass pharbers

	openssl x509 -req -CA snakeoil-ca-1.crt -CAkey snakeoil-ca-1.key -in $i.csr -out $i-ca1-signed.crt -days 9999 -CAcreateserial -passin pass:pharbers

	keytool -keystore kafka.$i.keystore.jks -alias CARoot -import -file snakeoil-ca-1.crt -storepass pharbers -keypass pharbers

	keytool -keystore kafka.$i.keystore.jks -alias $i -import -file $i-ca1-signed.crt -storepass pharbers -keypass pharbers

	# Create truststore and import the CA cert.
	keytool -keystore kafka.$i.truststore.jks -alias CARoot -import -file snakeoil-ca-1.crt -storepass pharbers -keypass pharbers

  echo "pharbers" > ${i}_sslkey_creds
  echo "pharbers" > ${i}_keystore_creds
  echo "pharbers" > ${i}_truststore_creds
done
