import requests

def test_endpoint(url):
    print(f"Testing {url}")
    try:
        res = requests.get(url, timeout=10)
        print("Status:", res.status_code)
        if res.status_code == 200:
            print("Response:", str(res.text)[:200])
        else:
            print("Failed")
    except Exception as e:
        print("Error:", str(e))
    print("-" * 40)

test_endpoint("https://dps.psx.com.pk/api/index/companies")
test_endpoint("https://dps.psx.com.pk/api/companies")
test_endpoint("https://sarmaaya.pk/api/market/companies")
test_endpoint("https://sarmaaya.pk/api/symbols")
