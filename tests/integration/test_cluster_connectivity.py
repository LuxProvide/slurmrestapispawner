import json
import os
import unittest
import urllib.error
import urllib.request


def _env(name: str, default: str = "") -> str:
    return os.environ.get(name, default).strip()


def _request_json(url: str, user: str, token: str, timeout: int = 15):
    headers = {
        "Accept": "application/json",
        "X-SLURM-USER-NAME": user,
        "X-SLURM-USER-TOKEN": token,
    }
    req = urllib.request.Request(url=url, headers=headers, method="GET")
    with urllib.request.urlopen(req, timeout=timeout) as resp:
        body = resp.read().decode("utf-8", "replace")
        return resp.status, json.loads(body)


def _slurm_env():
    url = _env("SLURMRESTD_URL")
    token = _env("SLURM_JWT")
    user = _env("SLURM_USER", _env("USER", ""))
    api_version = _env("SLURM_API_VERSION", "v0.0.43")
    timeout = int(_env("SLURM_TIMEOUT", "15"))
    if not url or not token or not user:
        raise unittest.SkipTest(
            "Set SLURMRESTD_URL, SLURM_JWT and SLURM_USER (or USER) to run integration tests."
        )
    return url.rstrip("/"), token, user, api_version, timeout


class TestClusterConnectivity(unittest.TestCase):
    def test_openapi_reachable(self):
        base_url, token, user, api_version, timeout = _slurm_env()
        status, payload = _request_json(f"{base_url}/openapi/v3", user, token, timeout)
        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)

        paths = payload.get("paths")
        path_keys = list(paths.keys()) if isinstance(paths, dict) else []
        self.assertTrue(
            any(api_version in p for p in path_keys),
            f"Did not find {api_version} in /openapi/v3 paths",
        )

    def test_jobs_endpoint_reachable(self):
        base_url, token, user, api_version, timeout = _slurm_env()
        try:
            status, payload = _request_json(f"{base_url}/slurm/{api_version}/jobs", user, token, timeout)
        except urllib.error.HTTPError as e:
            detail = e.read().decode("utf-8", "replace")
            self.fail(f"HTTP {e.code} calling jobs endpoint: {detail}")

        self.assertEqual(status, 200)
        self.assertIsInstance(payload, dict)
        # Slurm usually returns jobs plus metadata objects.
        self.assertTrue("jobs" in payload or "errors" in payload or "warnings" in payload)


if __name__ == "__main__":
    unittest.main()
