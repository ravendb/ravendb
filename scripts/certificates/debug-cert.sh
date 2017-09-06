#!/bin/bash

# openssl s_client -connect 172.20.53.119:8080
openssl s_client -connect $1
