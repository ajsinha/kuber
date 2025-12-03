# Copyright © 2025-2030, All Rights Reserved
# Ashutosh Sinha | Email: ajsinha@gmail.com
#
# Legal Notice: This module and the associated software architecture are proprietary
# and confidential. Unauthorized copying, distribution, modification, or use is
# strictly prohibited without explicit written permission from the copyright holder.

"""
Kuber Python Client
===================

A Python client for the Kuber Distributed Cache system.

Example usage:
    >>> from kuber import KuberClient
    >>> client = KuberClient('localhost', 6380)
    >>> client.set('key', 'value')
    >>> client.get('key')
    'value'
    >>> client.close()

Or using context manager:
    >>> with KuberClient('localhost', 6380) as client:
    ...     client.set('key', 'value')
    ...     print(client.get('key'))
"""

from .client import KuberClient, KuberException

__version__ = "1.0.3"
__author__ = "Ashutosh Sinha"
__email__ = "ajsinha@gmail.com"

__all__ = ["KuberClient", "KuberException"]
