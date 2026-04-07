import requests
import time


class DifyClient:
    def __init__(self, base_url, api_key):
        self.base_url = base_url.rstrip("/")
        self.api_key = api_key
        self.headers = {
            "Authorization": f"Bearer {api_key}",
            "Content-Type": "application/json",
        }

    def run_workflow(self, inputs, max_retries=3):
        url = f"{self.base_url}/workflows/run"
        payload = {
            "inputs": inputs,
            "response_mode": "blocking",
            "user": "doomscroll-agent",
        }

        for attempt in range(max_retries):
            try:
                resp = requests.post(url, json=payload, headers=self.headers, timeout=90)
                resp.raise_for_status()
                data = resp.json()

                outputs = data.get("data", {}).get("outputs", {})
                if len(outputs) == 1:
                    return list(outputs.values())[0]
                return outputs

            except requests.exceptions.RequestException as e:
                if attempt < max_retries - 1:
                    wait = 2 ** (attempt + 1)
                    print(f"  Dify request failed ({e}), retrying in {wait}s...")
                    time.sleep(wait)
                else:
                    raise