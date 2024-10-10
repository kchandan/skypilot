"""A script that generates the Denvr Cloud catalog for all clusters.

Usage:
    python fetch_denvr_cloud.py [-h] [--username USERNAME] [--password PASSWORD]
                                [--output OUTPUT_PATH]

If neither --username nor --password are provided, the script will look for
the Denvr Cloud credentials in the environment variables `DENVR_CLOUD_USER_EMAIL`
and `DENVR_CLOUD_PASSWORD`.
"""

import argparse
import csv
import json
import os
from typing import Optional, Tuple

import requests

# Denvr Cloud API Endpoints
AUTH_ENDPOINT = 'https://api.cloud.denvrdata.com/api/TokenAuth/Authenticate'
GET_AVAILABILITY_ENDPOINT_TEMPLATE = 'https://api.cloud.denvrdata.com/api/v1/servers/virtual/GetAvailability?cluster={cluster}&resourcePool=on-demand'
DEFAULT_OUTPUT_PATH = 'denvr/vms.csv'

# Dictionary for mapping known GPU types to their memory in MiB
GPU_TO_MEMORY = {
    'A100': 40960,
    'A100_80GB': 81920,
    'V100': 16384,
    'T4': 16384,
    'P100': 16384,
    'K80': 12288,
    'H100': 81920,
    'GENERAL': None
}

# List of known clusters (regions) for Denvr Cloud
CLUSTERS = [
    'Msc1',
    'Hou1'
]


def authenticate(username: str, password: str) -> Tuple[str, str]:
    """Authenticate with Denvr Cloud and return the access token and refresh token."""
    data = {
        "userNameOrEmailAddress": username,
        "password": password
    }
    headers = {"Content-Type": "application/json-patch+json"}
    response = requests.post(AUTH_ENDPOINT, headers=headers, json=data)
    response.raise_for_status()  # Raise an error if authentication fails
    auth_data = response.json()
    return auth_data['result']['accessToken'], auth_data['result']['refreshToken']


def fetch_vms_for_cluster(cluster: str, access_token: str, refresh_token: str) -> list:
    """Fetch the list of virtual machines for a specific cluster using both tokens."""
    headers = {
        'Authorization': f'Bearer {access_token}',
        'encryptedAccessToken': refresh_token,
        'Content-Type': 'application/json'
    }
    endpoint = GET_AVAILABILITY_ENDPOINT_TEMPLATE.format(cluster=cluster)
    response = requests.get(endpoint, headers=headers)
    response.raise_for_status()  # Raise an error if the request fails
    return response.json().get('result', {}).get('items', [])


def create_catalog(access_token: str, refresh_token: str, output_path: str) -> None:
    """Fetch the list of virtual machines for all clusters and save as a CSV."""
    with open(output_path, mode='w', encoding='utf-8') as f:
        writer = csv.writer(f, delimiter=',', quotechar='"')
        writer.writerow([
            'Configuration', 'Cluster', 'Resource Pool', 'Type', 'Price',
            'Available', 'Count', 'MaxCount', 'GPU_Type', 'GPU_Memory(MiB)'
        ])

        for cluster in CLUSTERS:
            print(f"Fetching VM availability for cluster: {cluster}...")
            vms = fetch_vms_for_cluster(cluster, access_token, refresh_token)
            for vm in vms:
                configuration = vm.get('configuration')
                cluster_name = vm.get('cluster')
                rpool = vm.get('rpool')
                vm_type = vm.get('type')
                price = vm.get('price')
                available = vm.get('available')
                count = vm.get('count')
                max_count = vm.get('maxCount')

                # Parse GPU information from the configuration name if available
                gpu_type = None
                for gpu in GPU_TO_MEMORY.keys():
                    if gpu in configuration:
                        gpu_type = gpu
                        break

                gpu_memory = GPU_TO_MEMORY.get(gpu_type, 0) if gpu_type else 0

                writer.writerow([
                    configuration,
                    cluster_name,
                    rpool,
                    vm_type,
                    price,
                    available,
                    count,
                    max_count,
                    gpu_type,
                    gpu_memory
                ])
    print(f'Denvr Cloud catalog saved to {output_path}')


def get_credentials(cmdline_args: argparse.Namespace) -> Tuple[str, str]:
    """Get Denvr Cloud username and password from command-line or environment variables."""
    username = cmdline_args.username or os.getenv('DENVR_CLOUD_USER_EMAIL')
    password = cmdline_args.password or os.getenv('DENVR_CLOUD_PASSWORD')

    assert username is not None and password is not None, (
        "Denvr Cloud credentials must be provided via --username/--password or "
        "through the DENVR_CLOUD_USER_EMAIL and DENVR_CLOUD_PASSWORD environment variables."
    )
    return username, password


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument('--username', help='Denvr Cloud username (email address).')
    parser.add_argument('--password', help='Denvr Cloud password.')
    parser.add_argument('--output', help='Path to output CSV file.', default=DEFAULT_OUTPUT_PATH)
    args = parser.parse_args()

    os.makedirs(os.path.dirname(args.output), exist_ok=True)

    # Get credentials from command-line arguments or environment variables
    user, pwd = get_credentials(args)
    
    # Authenticate and get the access token and refresh token
    access_token, refresh_token = authenticate(user, pwd)
    
    # Create the Denvr Cloud catalog for all clusters
    create_catalog(access_token, refresh_token, args.output)
