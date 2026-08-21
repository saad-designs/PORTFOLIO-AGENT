import requests
import json
import re

url = "https://dps.psx.com.pk/company/MEBL"
res = requests.get(url)
print("status:", res.status_code)

# Check for a Nuxt JS state
match = re.search(r'window\.__NUXT__=(.*?);</script>', res.text)
if match:
    print("Found Nuxt state!")
    nuxt_data = match.group(1)
    # just print first 500 chars to see what it looks like
    print(nuxt_data[:500])
else:
    print("No Nuxt state found in HTML.")

# Try potential direct API endpoint
api_url = "https://dps.psx.com.pk/api/company/MEBL"
api_res = requests.get(api_url)
print(f"Testing {api_url} -> status: {api_res.status_code}")
if api_res.status_code == 200:
    print(api_res.text[:300])

api_url2 = "https://dps.psx.com.pk/timeseries/int/MEBL"
api_res2 = requests.get(api_url2)
print(f"Testing {api_url2} -> status: {api_res2.status_code}")
if api_res2.status_code == 200:
    print(api_res2.text[:300])
