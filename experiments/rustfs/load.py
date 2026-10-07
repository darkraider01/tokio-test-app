import hmac
import hashlib
import datetime
import urllib.parse
import http.client
import time
import threading
import math
from concurrent.futures import ThreadPoolExecutor


def percentiles(values):
    if not values:
        return None
    values = sorted(values)
    return {name: values[max(0, math.ceil(q * len(values)) - 1)]
            for name, q in (("p50", .5), ("p95", .95), ("p99", .99), ("max", 1))}

def sign_s3(method, url_path, query_params, headers, payload, access_key, secret_key, region="us-east-1"):
    now = datetime.datetime.now(datetime.timezone.utc)
    amz_date = now.strftime('%Y%m%dT%H%M%SZ')
    date_stamp = now.strftime('%Y%m%d')

    if isinstance(payload, str):
        payload = payload.encode('utf-8')
    payload_hash = hashlib.sha256(payload).hexdigest()

    headers['x-amz-date'] = amz_date
    headers['x-amz-content-sha256'] = payload_hash

    sorted_header_keys = sorted(k.lower() for k in headers.keys())
    canonical_headers = "".join(f"{k}:{headers[next(orig for orig in headers if orig.lower() == k)].strip()}\n" for k in sorted_header_keys)
    signed_headers = ";".join(sorted_header_keys)

    canonical_query = urllib.parse.urlencode(sorted(query_params.items())) if query_params else ""
    canonical_uri = urllib.parse.quote(url_path, safe='/-_.~')

    canonical_request = f"{method}\n{canonical_uri}\n{canonical_query}\n{canonical_headers}\n{signed_headers}\n{payload_hash}"
    canonical_request_hash = hashlib.sha256(canonical_request.encode('utf-8')).hexdigest()

    credential_scope = f"{date_stamp}/{region}/s3/aws4_request"
    string_to_sign = f"AWS4-HMAC-SHA256\n{amz_date}\n{credential_scope}\n{canonical_request_hash}"

    def sign(key, msg):
        return hmac.new(key, msg.encode('utf-8'), hashlib.sha256).digest()

    k_date = sign(('AWS4' + secret_key).encode('utf-8'), date_stamp)
    k_region = sign(k_date, region)
    k_service = sign(k_region, "s3")
    k_signing = sign(k_service, "aws4_request")
    signature = hmac.new(k_signing, string_to_sign.encode('utf-8'), hashlib.sha256).hexdigest()

    auth_header = f"AWS4-HMAC-SHA256 Credential={access_key}/{credential_scope}, SignedHeaders={signed_headers}, Signature={signature}"
    headers['Authorization'] = auth_header
    return headers

class S3Client:
    def __init__(self, host="127.0.0.1", port=9000, access_key="rustfsadmin", secret_key="rustfsadmin"):
        self.host = host
        self.port = port
        self.access_key = access_key
        self.secret_key = secret_key

    def create_bucket(self, bucket_name):
        conn = http.client.HTTPConnection(self.host, self.port, timeout=10)
        headers = {'host': f'{self.host}:{self.port}'}
        signed = sign_s3('PUT', f'/{bucket_name}', {}, headers, b'', self.access_key, self.secret_key)
        try:
            conn.request('PUT', f'/{bucket_name}', body=b'', headers=signed)
            resp = conn.getresponse()
            resp.read()
            return resp.status in (200, 409)
        finally:
            conn.close()

    def put_object(self, bucket_name, object_key, payload_bytes):
        t0 = time.monotonic_ns()
        conn = http.client.HTTPConnection(self.host, self.port, timeout=30)
        headers = {
            'host': f'{self.host}:{self.port}',
            'content-length': str(len(payload_bytes)),
            'content-type': 'application/octet-stream',
        }
        try:
            signed = sign_s3('PUT', f'/{bucket_name}/{object_key}', {}, headers, payload_bytes, self.access_key, self.secret_key)

            t_req_start = time.monotonic_ns()
            conn.request('PUT', f'/{bucket_name}/{object_key}', body=payload_bytes, headers=signed)
            t_write_done = time.monotonic_ns()

            resp = conn.getresponse()
            request_id = resp.getheader("x-amz-request-id")
            t_headers_done = time.monotonic_ns()
            resp.read()
            t_done = time.monotonic_ns()
            status = resp.status
        finally:
            conn.close()

        return {
            'status': status,
            'server_request_id': request_id,
            'total_ms': (t_done - t0) / 1_000_000.0,
            'write_ms': (t_write_done - t_req_start) / 1_000_000.0,
            'response_headers_wait_ms': (t_headers_done - t_write_done) / 1_000_000.0,
            'response_wait_ms': (t_done - t_write_done) / 1_000_000.0,
        }


def run_tier(client, bucket, payload, name, duration, concurrency=None, rate=None,
             max_active=64):
    start = time.monotonic_ns()
    start_realtime = time.time_ns()
    deadline = start + int(duration * 1e9)
    records = []
    lock = threading.Lock()

    def request(req_id, scheduled_ns):
        attempted_ns = time.monotonic_ns()
        key = f"{name}/{req_id}.bin"
        record = {"key": key, "scheduled_ns": scheduled_ns,
                  "attempted_ns": attempted_ns, "attempted_realtime_ns": time.time_ns(),
                  "launch_lateness_ms": (attempted_ns - scheduled_ns) / 1e6}
        try:
            record.update(client.put_object(bucket, key, payload))
        except Exception as error:
            record.update(status=None, error=str(error))
        record["completed_ns"] = time.monotonic_ns()
        record["attempt_to_completion_ms"] = (record["completed_ns"] - attempted_ns) / 1e6
        record["scheduled_to_completion_ms"] = (record["completed_ns"] - scheduled_ns) / 1e6
        with lock:
            records.append(record)

    if rate is None:
        def worker(worker_id):
            req_id = worker_id
            while time.monotonic_ns() < deadline:
                request(req_id, time.monotonic_ns())
                req_id += concurrency

        with ThreadPoolExecutor(max_workers=concurrency) as pool:
            futures = [pool.submit(worker, i) for i in range(concurrency)]
            for future in futures:
                future.result()
    else:
        slots = threading.BoundedSemaphore(max_active)

        def admitted_request(req_id, scheduled_ns):
            try:
                request(req_id, scheduled_ns)
            finally:
                slots.release()

        with ThreadPoolExecutor(max_workers=max_active) as pool:
            futures = []
            for req_id in range(int(rate * duration)):
                scheduled_ns = start + int(req_id * 1e9 / rate)
                remaining = (scheduled_ns - time.monotonic_ns()) / 1e9
                if remaining > 0:
                    time.sleep(remaining)
                if not slots.acquire(blocking=False):
                    with lock:
                        records.append({"key": f"{name}/{req_id}.bin",
                                        "scheduled_ns": scheduled_ns, "client_shed": True})
                    continue
                futures.append(pool.submit(admitted_request, req_id, scheduled_ns))
            remaining = (deadline - time.monotonic_ns()) / 1e9
            if remaining > 0:
                time.sleep(remaining)
            for future in futures:
                future.result()

    end = time.monotonic_ns()
    attempts = [r for r in records if not r.get("client_shed")]
    successes = [r for r in attempts if r["status"] == 200]
    return {
        "tier": name, "mode": "closed_loop" if rate is None else "open_loop_with_client_cap",
        "concurrency": concurrency, "target_rps": rate, "max_active": max_active,
        "start_realtime_ns": start_realtime,
        "generation_end_realtime_ns": start_realtime + deadline - start,
        "end_realtime_ns": time.time_ns(), "generation_s": duration,
        "elapsed_including_drain_s": (end - start) / 1e9,
        "drain_s": max(0, end - deadline) / 1e9,
        "planned_arrivals": len(records), "attempted": len(attempts),
        "completed_ok": len(successes), "failed": len(attempts) - len(successes),
        "client_shed": len(records) - len(attempts),
        "attempts_during_generation_rps": sum(r["attempted_ns"] < deadline for r in attempts) / duration,
        "completions_during_generation_rps": sum(r["completed_ns"] <= deadline for r in successes) / duration,
        "throughput_including_drain_rps": len(successes) / ((end - start) / 1e9),
        "latency_ms": percentiles([r["attempt_to_completion_ms"] for r in successes]),
        "scheduled_to_completion_ms": percentiles([r["scheduled_to_completion_ms"] for r in successes]),
        "launch_lateness_ms": percentiles([r["launch_lateness_ms"] for r in attempts]),
        "requests": sorted(records, key=lambda r: r["scheduled_ns"]),
    }
