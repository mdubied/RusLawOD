import requests
import os

# Read URLs from data.txt
with open('../data.txt', 'r') as file:
    urls = [line.strip() for line in file if line.strip()]

for url in urls:
    try:
        # Send HEAD request to get headers only
        response = requests.head(url, allow_redirects=True, timeout=10)
        size_bytes = int(response.headers.get('Content-Length', 0))
        size_mb = size_bytes / (1024 * 1024)
        print(f"{url} → {size_mb:.2f} MB")
    except Exception as e:
        print(f"Failed to get size for {url}: {e}")


# Download each XML file
save_dir = "corpus_xml_full"
os.makedirs(save_dir, exist_ok=True)

with open("../data.txt", "r") as file:
    urls = [line.strip() for line in file if line.strip()]

total = len(urls)

for i, url in enumerate(urls, 1):
    filename = os.path.join(save_dir, os.path.basename(url))

    try:
        response = requests.get(url, timeout=20)
        if response.status_code == 200:
            with open(filename, 'wb') as f:
                f.write(response.content)
            size_kb = len(response.content) / 1024
            print(f"[{i}/{total}] Downloaded: {os.path.basename(url)} ({size_kb:.1f} KB)")
        else:
            print(f"[{i}/{total}] Failed ({response.status_code}): {url}")
    except Exception as e:
        print(f"[{i}/{total}] Error: {url} → {e}")