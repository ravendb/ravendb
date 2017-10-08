#!/bin/bash

check_errs()
{
  if [ "${1}" -ne "0" ]; then
    echo "ERROR # ${1} : ${2}"
    exit ${1}
  fi
}

if [ "$#" -eq "1" ]; then
	export SAN=$1
else
	echo please supply a DNS name in the first argument
	exit
fi

BASIC_PATH=$HOME/ravendb/certs
CA_PATH=${BASIC_PATH}/ca
INTERMEDIATE_PATH=${CA_PATH}/intermediate

echo "Cleaning ${BASIC_PATH}..."
rm -rf ${BASIC_PATH}

mkdir -p ${CA_PATH}/certs
mkdir -p ${CA_PATH}/newcerts
mkdir -p ${CA_PATH}/private
mkdir -p ${INTERMEDIATE_PATH}/certs
mkdir -p ${INTERMEDIATE_PATH}/csr
mkdir -p ${INTERMEDIATE_PATH}/newcerts
mkdir -p ${INTERMEDIATE_PATH}/private

cp openssl_ca.cnf ${CA_PATH}/openssl.cnf
cp openssl_intermediate.cnf ${INTERMEDIATE_PATH}/openssl.cnf

chmod 700 ${CA_PATH}/private
chmod 700 ${INTERMEDIATE_PATH}/private

touch ${CA_PATH}/index.txt
echo 1000 > ${CA_PATH}/serial

echo "Generating a self-signed Certificate Authority:"
openssl genrsa -aes256 -out ${CA_PATH}/private/ca.key.pem 4096
check_errs $?

chmod 400 ${CA_PATH}/private/ca.key.pem
openssl req -config ${CA_PATH}/openssl.cnf -key ${CA_PATH}/private/ca.key.pem -new -x509 -days 7300 -sha256 -extensions v3_ca -out ${CA_PATH}/certs/ca.cert.pem
check_errs $?

chmod 444 ${CA_PATH}/certs/ca.cert.pem
openssl x509 -noout -text -in ${CA_PATH}/certs/ca.cert.pem
check_errs $?

touch ${INTERMEDIATE_PATH}/index.txt
echo 1000 > ${INTERMEDIATE_PATH}/serial
echo 1000 > ${INTERMEDIATE_PATH}/crlnumber

echo "Generating an Intermediate Certificate Authority (Server certificate):"
openssl genrsa -aes256 -out ${INTERMEDIATE_PATH}/private/intermediate.key.pem 4096
check_errs $?

chmod 400 ${INTERMEDIATE_PATH}/private/intermediate.key.pem
openssl req -config ${INTERMEDIATE_PATH}/openssl.cnf -new -sha256 -key ${INTERMEDIATE_PATH}/private/intermediate.key.pem -out ${INTERMEDIATE_PATH}/csr/intermediate.csr.pem
check_errs $?

openssl ca -config ${CA_PATH}/openssl.cnf -extensions ravendb_custom_server -days 3650 -notext -md sha256 -in ${INTERMEDIATE_PATH}/csr/intermediate.csr.pem -out ${INTERMEDIATE_PATH}/certs/intermediate.cert.pem
check_errs $?

chmod 444 ${INTERMEDIATE_PATH}/certs/intermediate.cert.pem
openssl x509 -noout -text -in ${INTERMEDIATE_PATH}/certs/intermediate.cert.pem
check_errs $?

openssl verify -CAfile ${CA_PATH}/certs/ca.cert.pem ${INTERMEDIATE_PATH}/certs/intermediate.cert.pem
check_errs $?

cat ${INTERMEDIATE_PATH}/certs/intermediate.cert.pem ${CA_PATH}/certs/ca.cert.pem > ${INTERMEDIATE_PATH}/certs/ca-chain.cert.pem
chmod 444 ${INTERMEDIATE_PATH}/certs/ca-chain.cert.pem

echo "Exporting certificate ${BASIC_PATH}/ca.pfx"
openssl pkcs12 -export -in ${CA_PATH}/certs/ca.cert.pem -inkey ${CA_PATH}/private/ca.key.pem -out ${BASIC_PATH}/ca.pfx -certfile ${CA_PATH}/certs/ca.cert.pem
check_errs $?

echo "Exporting certificate ${BASIC_PATH}/server.pfx"
openssl pkcs12 -export -in ${INTERMEDIATE_PATH}/certs/intermediate.cert.pem -inkey ${INTERMEDIATE_PATH}/private/intermediate.key.pem  -out ${BASIC_PATH}/server.pfx -certfile ${CA_PATH}/certs/ca.cert.pem
check_errs $?

echo "Done."