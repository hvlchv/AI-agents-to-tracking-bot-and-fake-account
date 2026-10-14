# TODO: Implement deterministic REES46 sample creation.
import argparse
import csv
import gzip
import hashlib
import time
import json
from datetime import datetime, timezone
from pathlib import Path

FIELDS = [
    'event_id', 'event_time', 'source', 'user_id', 'dataset_version', 'event_type', 
    'product_id', 'category_id', 'category_code', 'brand', 'price',    
    'ip_token', 'currency', 'device_id', 'coupon_id', 'attributes'
]

def utc(value):
    result = datetime.fromisoformat(value.replace('UTC', '+00:00').replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError("Must be include timezone")
    return result.astimezone(timezone.utc)

def digest(path):
    h = hashlib.sha256()
    with path.open('rb') as f:
        for block in iter(lambda: f.read(1024 * 1024), b''):
            h.update(block)
    return h.hexdigest()

def main():
    p = argparse.ArgumentParser()
    p.add_argument('--input', required=True)
    p.add_argument('--output-dir', required=True)
    p.add_argument('--source-file', default='2019-Oct.csv')
    p.add_argument('--dataset-version', default='rees46-multi-Oct2019-v1')
    p.add_argument('--start', default='2019-10-01 00:00:00 UTC')

if __name__ == '__main__':
    main()