import os
import redis
import time
import random
from datetime import datetime

# Kết nối Redis (host có thể override bằng biến môi trường REDIS_HOST)
REDIS_HOST = os.getenv('REDIS_HOST', 'redis')
r = redis.Redis(host=REDIS_HOST, port=6379, decode_responses=False)

# Tạo time series nếu chưa có
def create_timeseries():
    try:
        r.execute_command('TS.CREATE', 'ts:txn:count', 'RETENTION', '600000')
        print("✓ Created ts:txn:count")
    except:
        print("• ts:txn:count already exists")
    
    try:
        r.execute_command('TS.CREATE', 'ts:txn:fraud', 'RETENTION', '600000')
        print("✓ Created ts:txn:fraud")
    except:
        print("• ts:txn:fraud already exists")
    
    try:
        r.execute_command('TS.CREATE', 'ts:txn:pred', 'RETENTION', '600000')
        print("✓ Created ts:txn:pred")
    except:
        print("• ts:txn:pred already exists")
    
    try:
        r.execute_command('TS.CREATE', 'ts:txn:amount_minor', 'RETENTION', '600000')
        print("✓ Created ts:txn:amount_minor")
    except:
        print("• ts:txn:amount_minor already exists")

def generate_metrics():
    """Generate fake metrics and write to Redis time series"""
    
    # Tạo time series
    create_timeseries()
    
    print("\n🚀 Starting to generate fake metrics...\n")
    
    cycle = 0
    while True:
        timestamp = int(time.time() * 1000)  # milliseconds
        
        # Tạo pattern: peak hours có nhiều transaction hơn
        hour = datetime.now().hour
        is_peak = 9 <= hour <= 21  # Peak hours
        
        # Generate fake data
        # Total transactions: 20-100 per interval (more during peak)
        base_txn = 50 if is_peak else 30
        txn_count = random.randint(base_txn - 20, base_txn + 50)
        
        # Fraud: 5-15% of transactions
        fraud_rate = random.uniform(0.05, 0.15)
        fraud_count = int(txn_count * fraud_rate)
        
        # Predicted fraud: similar to actual but with some variance
        pred_fraud = max(0, fraud_count + random.randint(-2, 3))
        
        # Transaction amounts: $10-$500 per transaction
        amount_minor = sum(random.randint(1000, 50000) for _ in range(txn_count))
        
        # Write to Redis time series
        try:
            r.execute_command('TS.ADD', 'ts:txn:count', timestamp, txn_count)
            r.execute_command('TS.ADD', 'ts:txn:fraud', timestamp, fraud_count)
            r.execute_command('TS.ADD', 'ts:txn:pred', timestamp, pred_fraud)
            r.execute_command('TS.ADD', 'ts:txn:amount_minor', timestamp, amount_minor)
            
            # Log
            cycle += 1
            if cycle % 10 == 0:
                print(f"[{datetime.now().strftime('%H:%M:%S')}] "
                      f"Cycle {cycle} | "
                      f"Txn: {txn_count:3d} | "
                      f"Fraud: {fraud_count:2d} ({fraud_rate*100:.1f}%) | "
                      f"Pred: {pred_fraud:2d} | "
                      f"Amount: ${amount_minor/100:.2f}")
            
        except Exception as e:
            print(f"❌ Error writing to Redis: {e}")
        
        # Wait 1 second before next data point
        time.sleep(1)


def connect_with_retry(max_attempts=None):
    """Try to connect to Redis, retrying with exponential backoff until success.

    Returns True if connected, False if max_attempts is reached.
    """
    attempt = 0
    delay = 1
    while True:
        try:
            if r.ping():
                print(f"✓ Connected to Redis at {REDIS_HOST}:6379")
                return True
        except Exception as e:
            attempt += 1
            print(f"⚠️  Redis not reachable at {REDIS_HOST}:6379 (attempt {attempt}) — {e}")
            if max_attempts and attempt >= max_attempts:
                return False
            time.sleep(delay)
            delay = min(delay * 2, 10)

if __name__ == "__main__":
    print("=" * 60)
    print("📊 GRAFANA METRICS GENERATOR")
    print("=" * 60)
    print(f"Redis: {r.ping() and '✓ Connected' or '✗ Failed'}")
    print("=" * 60)
    
    try:
        generate_metrics()
    except KeyboardInterrupt:
        print("\n\n⏹️  Stopped by user")
    except Exception as e:
        print(f"\n❌ Error: {e}")
