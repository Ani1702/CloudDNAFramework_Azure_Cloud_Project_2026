import sys
from typing import Dict, Any

try:
    from azure.identity import ManagedIdentityCredential
    from azure.mgmt.containerinstance import ContainerInstanceManagementClient
    print("ACI Available: True")
except Exception as e:
    print(f"ACI Available: False, error: {e}")

try:
    cred = ManagedIdentityCredential()
    client = ContainerInstanceManagementClient(cred, "87117a52-08aa-465f-9c7e-5fa5b0815efd")
    print("Client initialized successfully.")
except Exception as e:
    print(f"Client init error: {e}")
