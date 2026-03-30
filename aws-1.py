import requests
import time
import json
from datetime import datetime
import random

# ================= CONFIGURATION =================
BASE_KEYS = [
    "DRET3GcgohidlQj2TFfGVqWB5YTenYO1",
    "N_vWCV2FygpTrFothrLGfQdRlsh3a8pA",
    "L2udhpBOmCwTcNveVYWYAjMwzZYmiH6o",
    "CzxFFW4318BxCrYj088wykFOnA3hCamv"
]

TARGET_URL = "https://api.polygon.io/v2/aggs/ticker/AAPL/prev"
OUTPUT_FILE = "valid_keys.txt"
DELAY_SECONDS = 0.15  # Slightly longer delay for deep scan

# ================= MUTATION ENGINE (DEEP SCAN) =================
def generate_combinations(base_key, depth=3):
    """Generates variations of the key to test."""
    variants = [base_key]  # Start with exact match
    
    # 1. Append numbers (0-99 for deeper scan)
    for i in range(100):
        variants.append(f"{base_key}{i}")
    
    # 2. Prefix common strings (more options)
    prefixes = ["poly_", "test_", "dev_", "prod_", "live_", "staging_", "api_"]
    for p in prefixes:
        variants.append(p + base_key)
        
    # 3. Middle insertions (_5, _99, etc.)
    for i in range(10):
        variants.append(f"{base_key}_{i}")
    
    # 4. Case variations (swap case of individual characters - limited to avoid too many)
    if len(base_key) > 8:
        mid_idx = len(base_key) // 2
        variants.append(base_key[:mid_idx] + base_key[mid_idx].upper() + base_key[mid_idx+1:])
    
    # 5. Common environment markers (often used in dev/test keys)
    env_markers = ["_prod", "_dev", "_test", "_live", "_staging"]
    for marker in env_markers:
        variants.append(base_key + marker)
        
    return list(set(variants))

# ================= TESTER ENGINE =================
def test_key(api_key):
    """Tests a single key against Polygon API."""
    url = f"{TARGET_URL}?apiKey={api_key}"
    try:
        response = requests.get(url, timeout=10)
        
        return {
            "key": api_key,
            "status_code": response.status_code,
            "valid": response.status_code == 200,
            "response_time_ms": response.elapsed.total_seconds() * 1000
        }
    except Exception as e:
        return {
            "key": api_key,
            "status_code": "ERROR",
            "valid": False,
            "error": str(e)
        }

# ================= MAIN EXECUTION =================
def main():
    print(f"🚀 Starting Polygon API Key Tester (Deep Scan)...")
    print(f"⏱️  Base URL: {TARGET_URL}")
    
    # Generate all combinations for each base key
    all_keys = []
    total_variants = 0
    
    for i, base in enumerate(BASE_KEYS):
        variants = generate_combinations(base)
        all_keys.extend(variants)
        total_variants += len(variants)
        
        print(f"\n🔑 Seed Key #{i+1}: {base[:25]}...")
        print(f"   Generated Variations: {len(variants)}")

    print(f"\n📦 Total keys to test: {len(all_keys)}")
    
    valid_results = []
    invalid_count = 0
    
    # Test each key with rate limiting (Polygon API has limits!)
    for i, api_key in enumerate(all_keys):
        result = test_key(api_key)
        
        if result["valid"]:
            print(f"✅ VALID: {api_key}")
            valid_results.append(result)
        else:
            # Only print errors occasionally to avoid spam (every 20th invalid key)
            if i % 20 == 0 or "ERROR" in str(result.get("status_code", "")):
                print(f"❌ INVALID ({result['status_code']}): {api_key}")
        
        # Rate limiting - Polygon API has rate limits (~10-20 req/sec)
        time.sleep(DELAY_SECONDS)
    
    # ================= SAVE RESULTS =================
    output_data = {
        "timestamp": datetime.now().isoformat(),
        "total_tested": len(all_keys),
        "valid_count": len(valid_results),
        "invalid_count": invalid_count,
        "results": valid_results
    }
    
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n💾 Results saved to {OUTPUT_FILE}")
    print(f"🎯 Found {len(valid_results)} valid keys!")

if __name__ == "__main__":
    main()
