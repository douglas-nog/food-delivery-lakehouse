#!/usr/bin/env bash
set -euo pipefail
# Dev convenience: auto-detect public IP and inject into apply.
# In production, infra runs via CI/CD with known egress/service-principal access.
MY_IP=$(curl -s -4 https://api.ipify.org)
echo ">> Detected IP: ${MY_IP}"
cd "$(dirname "${BASH_SOURCE[0]}")"
terraform apply -var="my_ip=${MY_IP}" "$@"