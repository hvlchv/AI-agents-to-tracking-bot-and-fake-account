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

# ham utc dung de dong bo thoi gian
def utc(value):
    result = datetime.fromisoformat(value.replace('UTC', '+00:00').replace('Z', '+00:00'))
    if result.tzinfo is None:
        raise ValueError("Must be include timezone")
    return result.astimezone(timezone.utc)

# ham digest dung de kiem tra dung luong file
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
    p.add_argument('--evaluation-start', default='2019-10-01 01:00:00 UTC')
    p.add_argument('--end', default='2019-10-02 01:00:00 UTC')
    p.add_argument('--buckets', type=int, default=1000)
    p.add_argument('--keep-buckets', type=int, default=100)
    p.add_argument('--max-rows', type=int, default=300000)
    p.add_argument('--seed', default='public-sample-v1')
    a = p.parse_args()
    
    start, evaluation_start, end = utc(a.start), utc(a.evaluation_start), utc(a.end)
    if not start < evaluation_start < end:
        raise ValueError('Expect start < evaluation_start < end')
    if not 0 < a.keep_buckets <= a.buckets or a.max_rows <= 0:
        raise ValueError('Invalid samping limits')
    src, out = Path(a.input), Path(a.output_dir)
    out.mkdir(exist_ok=True, parents=True)
    final, manifest_path = out/'event.csv', out/'manifest.json'
    if final.exists() or manifest_path.exists():
        raise FileExistsError('Use a new output directory; do not overwrite a completed sample')
    temp = out/'events.csv.partial'
    opener = gzip.open if src.suffix == 'gz' else open
    scanned = selected = warmup = 0
    min_time = max_time = None
    began = time.monotonic()
    
    
    
    
    
if __name__ == '__main__':
    main()