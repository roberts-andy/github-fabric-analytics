"""Small authenticated REST client; credentials stay in the Azure CLI cache."""

import json
import shutil
import subprocess
import time
import urllib.error
import urllib.request

BASE = "https://api.fabric.microsoft.com/v1"


def command_json(arguments):
    executable = shutil.which(arguments[0])
    if not executable:
        raise RuntimeError(f"Required command not found: {arguments[0]}")
    result = subprocess.run(
        [executable, *arguments[1:]], check=True, capture_output=True, encoding="utf-8-sig"
    )
    return json.loads(result.stdout)


class Fabric:
    def __init__(self, skill="e2e-medallion-architecture"):
        self.skill = skill
        self.tokens = {}

    def token(self, resource):
        cached = self.tokens.get(resource)
        if not cached or time.monotonic() - cached[0] > 1800:
            value = command_json([
                "az", "account", "get-access-token", "--resource", resource,
                "--output", "json",
            ])
            self.tokens[resource] = (time.monotonic(), value["accessToken"])
        return self.tokens[resource][1]

    def request(self, method, path, body=None, raw=None, headers=None, resource=None):
        url = path if path.startswith("https://") else BASE + path
        resource = resource or "https://api.fabric.microsoft.com"
        request_headers = {"Authorization": f"Bearer {self.token(resource)}"}
        if url.startswith(BASE):
            request_headers["x-ms-fabric-skill"] = self.skill
        if body is not None:
            raw = json.dumps(body).encode("utf-8")
            request_headers["Content-Type"] = "application/json"
        request_headers.update(headers or {})
        for attempt in range(5):
            request = urllib.request.Request(
                url, data=raw, headers=request_headers, method=method
            )
            try:
                with urllib.request.urlopen(request, timeout=120) as response:
                    data = response.read()
                    result = json.loads(data) if data else None
                    return response.status, dict(response.headers), result
            except urllib.error.HTTPError as error:
                detail = error.read().decode("utf-8", errors="replace")
                if error.code in (429, 503, 504) and method == "GET" and attempt < 4:
                    wait = int(error.headers.get("Retry-After", 5 * 2**attempt))
                    print(f"GET throttled ({error.code}); waiting {wait}s", flush=True)
                    time.sleep(wait)
                    continue
                raise RuntimeError(
                    f"{method} {url}: HTTP {error.code}: {detail}"
                ) from error
        raise RuntimeError(f"Retry limit reached: {method} {url}")

    def call(self, method, path, body=None, timeout=600):
        status, headers, result = self.request(method, path, body)
        if status != 202:
            return result
        location = next(
            (v for k, v in headers.items() if k.lower() == "location"), None
        )
        if not location:
            raise RuntimeError(f"202 response missing Location: {method} {path}")
        deadline = time.monotonic() + timeout
        wait = int(next(
            (v for k, v in headers.items() if k.lower() == "retry-after"), "5"
        ))
        while time.monotonic() < deadline:
            time.sleep(max(wait, 2))
            _, poll_headers, operation = self.request("GET", location)
            state = operation.get("status", "")
            if state == "Succeeded":
                # Definition updates have no result body.
                if "/updateDefinition" in path:
                    return operation
                return self.request("GET", location.rstrip("/") + "/result")[2]
            if state in ("Failed", "Cancelled"):
                raise RuntimeError(json.dumps(operation))
            wait = int(next(
                (v for k, v in poll_headers.items() if k.lower() == "retry-after"), "5"
            ))
        raise TimeoutError(f"Operation still pending at {location}; do not resubmit blindly")

    def list(self, path):
        values = []
        while path:
            page = self.call("GET", path)
            values.extend(page["value"])
            path = page.get("continuationUri")
            if page.get("continuationToken") and not path:
                raise RuntimeError("Pagination token present without a continuation URI")
        return values
