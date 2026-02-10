# Copyright © 2025-2030, All Rights Reserved
# Ashutosh Sinha | Email: ajsinha@gmail.com
#
# Legal Notice: This module and the associated software architecture are proprietary
# and confidential. Unauthorized copying, distribution, modification, or use is
# strictly prohibited without explicit written permission from the copyright holder.

"""
Kuber Python Client
===================

A Python client library for the Kuber Distributed Cache system.

Two clients are available:
1. KuberClient - Redis protocol client with Kuber extensions
2. KuberRestClient - HTTP REST API client

Redis Protocol Client Example:
    >>> from kuber import KuberClient
    >>> with KuberClient('localhost', 6380) as client:
    ...     # Basic operations
    ...     client.set('key', 'value')
    ...     print(client.get('key'))
    ...     
    ...     # Region operations
    ...     client.select_region('products')
    ...     client.json_set('prod:1', {'name': 'Widget', 'price': 29.99})
    ...     
    ...     # JSON search
    ...     results = client.json_search('$.price>20')

REST API Client Example:
    >>> from kuber import KuberRestClient
    >>> with KuberRestClient('localhost', 8080) as client:
    ...     # Basic operations
    ...     client.set('key', 'value')
    ...     print(client.get('key'))
    ...     
    ...     # Region operations
    ...     client.select_region('products')
    ...     client.json_set('prod:1', {'name': 'Widget', 'price': 29.99})
    ...     
    ...     # JSON search
    ...     results = client.json_search('$.price>20')

For comprehensive examples, see:
- examples/redis_protocol_example.py
- examples/rest_api_example.py
"""

from .client import KuberClient, KuberException
from .rest_client import KuberRestClient, KuberRestException

__version__ = "2.1.0"
__author__ = "Ashutosh Sinha"
__email__ = "ajsinha@gmail.com"

__all__ = [
    "KuberClient", 
    "KuberException",
    "KuberRestClient",
    "KuberRestException"
]
