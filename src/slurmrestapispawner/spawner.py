import re
import shlex
import time
import asyncio
import math
from typing import Any, Dict, Optional

from jupyterhub.spawner import Spawner
from traitlets import Bool, Dict as DictTrait, Int, Unicode


class SlurmRESTAPISpawner(Spawner):
    """Spawn single-user servers by submitting jobs to slurmrestd.

    Flow:
    1. Submit job via slurmrestd.
    2. Poll job state until RUNNING.
    3. Determine execution host from the job info.
    4. Return (ip, port) for JupyterHub to connect to.
    """

    slurmrestd_url = Unicode(
        "http://127.0.0.1:6820",
        config=True,
        help="Base URL for slurmrestd.",
    )

    slurm_api_version = Unicode(
        "v0.0.43",
        config=True,
        help="Slurm REST API version segment used in endpoint URLs.",
    )

    slurm_cluster = Unicode(
        "",
        config=True,
        help="Optional cluster name for job submission.",
    )

    token_env_var = Unicode(
        "SLURM_JWT",
        config=True,
        help="Environment variable containing a Slurm REST auth token.",
    )

    slurm_token = Unicode(
        "",
        config=True,
        help="Slurm REST auth token. If empty, token_env_var is used.",
    )
    slurm_user = Unicode(
        "",
        config=True,
        help="Slurm user override for REST requests. If empty, JupyterHub username is used.",
    )

    validate_cert = Bool(
        True,
        config=True,
        help="Validate TLS certificates for slurmrestd HTTPS calls.",
    )

    request_timeout = Int(
        30,
        config=True,
        help="Per-request timeout (seconds) for Slurm REST calls.",
    )

    debug_slurm_api = Bool(
        False,
        config=True,
        help="Enable verbose debug logging of Slurm REST requests/responses (sanitized).",
    )

    startup_poll_interval = Int(
        3,
        config=True,
        help="Seconds between Slurm job state checks during startup.",
    )

    execution_host_fallback = Unicode(
        "",
        config=True,
        help="Fallback hostname/IP if execution host cannot be inferred.",
    )

    # Job defaults
    partition = Unicode("", config=True, help="Slurm partition.")
    account = Unicode("", config=True, help="Slurm account.")
    qos = Unicode("", config=True, help="Slurm QoS.")
    time_limit = Unicode("01:00:00", config=True, help="Slurm time limit HH:MM:SS.")
    cpus_per_task = Int(1, config=True, help="Deprecated: not used in job submission.")
    mem_per_node = Unicode("2G", config=True, help="Deprecated: not used in job submission.")

    extra_job_fields = DictTrait(default_value={}, config=True, help="Deprecated: not used in job submission.")

    enable_user_options_form = Bool(
        True,
        config=True,
        help="If True, show a spawn form allowing users to override Slurm job options.",
    )

    job_id = Int(0)

    batch_script = Unicode(
        """#!/bin/bash
set -euo pipefail
{prologue}
exec {singleuser_cmd}
""",
        config=True,
        help=(
            "Batch script template rendered with: prologue, singleuser_cmd. "
            "singleuser_cmd already includes proper shell quoting."
        ),
    )

    def options_form(self, spawner=None):
        s = spawner or self
        if not s.enable_user_options_form:
            return ""
        return f"""
<label for="slurm-account">Account</label>
<input name="account" id="slurm-account" type="text" value="{s.account or ''}" />
<br/>
<label for="slurm-partition">Partition</label>
<input name="partition" id="slurm-partition" type="text" value="{s.partition or ''}" />
<br/>
<label for="slurm-qos">QoS</label>
<input name="qos" id="slurm-qos" type="text" value="{s.qos or ''}" />
<br/>
<label for="slurm-time-limit">Time limit</label>
<input name="time_limit" id="slurm-time-limit" type="text" value="{s.time_limit}" placeholder="02:00:00" />
<br/>
<label for="slurm-token">Token</label>
<input name="token" id="slurm-token" type="password" value="" placeholder="SLURM JWT token" />
<br/>
<label for="slurm-user">Slurm user</label>
<input name="slurm_user" id="slurm-user" type="text" value="{s.slurm_user or s.user.name}" />
"""

    def options_from_form(self, formdata):
        # JupyterHub sends each key as a list of submitted values.
        parsed = {}
        for key in ("account", "partition", "qos", "time_limit", "token", "slurm_user"):
            value = formdata.get(key, [""])
            if isinstance(value, list):
                value = value[0] if value else ""
            parsed[key] = str(value).strip()
        if self.debug_slurm_api:
            self.log.warning("Spawn form received raw=%s parsed=%s", formdata, parsed)
        return parsed

    async def apply_user_options(self, spawner, user_options):
        s = spawner or self
        if not isinstance(user_options, dict):
            return

        if s.debug_slurm_api:
            s.log.warning("Applying user options: %s", user_options)

        for key in ("account", "partition", "qos", "time_limit"):
            value = user_options.get(key, "")
            if value:
                setattr(s, key, str(value).strip())

        token = user_options.get("token", "")
        if token:
            s.slurm_token = str(token).strip()
        slurm_user = user_options.get("slurm_user", "")
        if slurm_user:
            s.slurm_user = str(slurm_user).strip()

    def _slurm_user_value(self) -> str:
        return self.slurm_user or self.user.name

    def _slurm_token_value(self) -> str:
        token = self.slurm_token
        if token:
            return token
        return self.get_env().get(self.token_env_var, "")

    def _api_version_suffix(self) -> str:
        # v0.0.40 -> v0040
        parts = self.slurm_api_version.strip().lstrip("v").split(".")
        if len(parts) != 3 or not all(p.isdigit() for p in parts):
            raise ValueError(
                f"Unsupported slurm_api_version format '{self.slurm_api_version}'. "
                "Expected like v0.0.40"
            )
        major, minor, patch = (int(p) for p in parts)
        if major != 0:
            raise ValueError(
                f"Unsupported major version in slurm_api_version '{self.slurm_api_version}'. "
                "Expected v0.x.y format."
            )
        return f"v{minor:02d}{patch:02d}"

    def _resolve_method_name(self, api: Any, op: str) -> str:
        preferred = f"slurm_{self._api_version_suffix()}_{op}"
        if hasattr(api, preferred):
            return preferred

        pattern = re.compile(rf"^slurm_(v\d+)_{re.escape(op)}$")
        available = []
        for name in dir(api):
            m = pattern.match(name)
            if m:
                available.append((int(m.group(1)[1:]), name))

        if not available:
            raise RuntimeError(f"slurmrestpy does not expose any method for operation '{op}'")

        available.sort(reverse=True)
        fallback_name = available[0][1]
        self.log.warning(
            "slurmrestpy does not expose method '%s'; falling back to '%s'. "
            "Adjust slurm_api_version or upgrade slurmrestpy.",
            preferred,
            fallback_name,
        )
        return fallback_name

    def _to_dict(self, value: Any) -> Dict[str, Any]:
        if value is None:
            return {}
        if isinstance(value, dict):
            return value
        if hasattr(value, "to_dict"):
            return value.to_dict()
        return {}

    def _sanitize_for_log(self, value: Any) -> Any:
        if isinstance(value, dict):
            redacted = {}
            for k, v in value.items():
                key = str(k).lower()
                if key in {"token", "authorization", "x-slurm-user-token"}:
                    redacted[k] = "***REDACTED***"
                elif key == "script" and isinstance(v, str):
                    # Keep size and small prefix for diagnostics without flooding logs.
                    snippet = v[:200].replace("\n", "\\n")
                    redacted[k] = f"<script len={len(v)} preview='{snippet}...'>"
                else:
                    redacted[k] = self._sanitize_for_log(v)
            return redacted
        if isinstance(value, list):
            return [self._sanitize_for_log(v) for v in value]
        return value

    def _uint_value(self, n: int) -> Dict[str, Any]:
        return {"set": True, "number": int(n)}

    def _parse_time_limit_minutes(self, value: str) -> int:
        raw = str(value).strip()
        if not raw:
            raise ValueError("time_limit cannot be empty")

        if raw.isdigit():
            return int(raw)

        m = re.match(r"^(?:(\d+)-)?(\d+):(\d+)(?::(\d+))?$", raw)
        if not m:
            raise ValueError(
                f"Unsupported time_limit format '{value}'. Expected minutes or [[DD-]HH:]MM:SS"
            )
        days_s, a_s, b_s, c_s = m.groups()
        days = int(days_s) if days_s else 0
        a = int(a_s)
        b = int(b_s)
        c = int(c_s) if c_s is not None else None
        if c is None:
            # HH:MM
            total_seconds = (days * 24 + a) * 3600 + b * 60
        else:
            # HH:MM:SS or DD-HH:MM:SS
            total_seconds = (days * 24 + a) * 3600 + b * 60 + c
        return int(math.ceil(total_seconds / 60.0))

    def _parse_memory_mebibytes(self, value: str) -> int:
        raw = str(value).strip()
        if not raw:
            raise ValueError("mem_per_node cannot be empty")

        if raw.isdigit():
            return int(raw)

        m = re.match(r"^(\d+)\s*([KMGTP])(?:i?B?)?$", raw, re.IGNORECASE)
        if not m:
            raise ValueError(
                f"Unsupported mem_per_node format '{value}'. Expected integer MiB or suffix K/M/G/T/P."
            )

        amount = int(m.group(1))
        unit = m.group(2).upper()
        scale_to_mib = {
            "K": 1 / 1024,
            "M": 1,
            "G": 1024,
            "T": 1024 * 1024,
            "P": 1024 * 1024 * 1024,
        }
        return int(math.ceil(amount * scale_to_mib[unit]))

    async def _slurm_call(self, op: str, *args):
        try:
            import slurmrestpy
        except Exception as e:
            raise RuntimeError(
                "slurmrestpy is required but not installed. Install with: pip install slurmrestpy"
            ) from e

        def _run():
            cfg = slurmrestpy.Configuration(host=self.slurmrestd_url.rstrip("/"))
            cfg.verify_ssl = self.validate_cert
            cfg.api_key["user"] = self._slurm_user_value()
            token = self._slurm_token_value()
            if token:
                cfg.api_key["token"] = token
                cfg.access_token = token
            with slurmrestpy.ApiClient(cfg) as api_client:
                api = slurmrestpy.SlurmApi(api_client)
                method_name = self._resolve_method_name(api, op)
                method = getattr(api, method_name)
                if self.debug_slurm_api:
                    self.log.warning(
                        "Slurm API call: method=%s op=%s args=%s",
                        method_name,
                        op,
                        self._sanitize_for_log(list(args)),
                    )
                result = method(*args, _request_timeout=self.request_timeout)
                if self.debug_slurm_api:
                    self.log.warning(
                        "Slurm API response: method=%s body=%s",
                        method_name,
                        self._sanitize_for_log(self._to_dict(result)),
                    )
                return result

        try:
            return await asyncio.to_thread(_run)
        except Exception as e:
            if self.debug_slurm_api:
                self.log.exception("Slurm API call failed for op=%s", op)
            if "ApiException" in e.__class__.__name__:
                status = getattr(e, "status", "unknown")
                reason = getattr(e, "reason", str(e))
                body = getattr(e, "body", "")
                raise RuntimeError(f"Slurm REST request failed ({status}): {reason} {body}") from e
            raise

    def _singleuser_command(self) -> str:
        argv = list(self.cmd) + list(self.get_args())
        argv.extend(["--ip=0.0.0.0", f"--port={self.port}"])
        return shlex.join(argv)

    def _render_script(self) -> str:
        prologue = "\n".join(self.get_env_exports())
        return self.batch_script.format(
            prologue=prologue,
            singleuser_cmd=self._singleuser_command(),
        )

    def get_env_exports(self):
        exports = []
        for key, value in self.get_env().items():
            exports.append(f"export {shlex.quote(str(key))}={shlex.quote(str(value))}")
        return exports

    def _first_hostname_from_nodelist(self, raw: str) -> str:
        raw = (raw or "").strip()
        if not raw:
            return ""

        # Example: node[001-004,010] -> node001
        m = re.match(r"^([A-Za-z0-9_.-]+)\[([^\]]+)\]$", raw)
        if m:
            prefix, ranges = m.groups()
            first = ranges.split(",")[0]
            start = first.split("-")[0]
            if start.isdigit():
                width = len(start)
                return f"{prefix}{int(start):0{width}d}"
            return f"{prefix}{start}"

        # Example: node001,node002 -> node001
        return raw.split(",")[0]

    def _job_state(self, job: Dict[str, Any]) -> str:
        return (job.get("job_state") or job.get("job_state_current") or "").upper()

    def _job_host(self, job: Dict[str, Any]) -> str:
        for key in ("nodes", "batch_host", "alloc_node", "node_list"):
            value = job.get(key)
            if value:
                return self._first_hostname_from_nodelist(str(value))
        return ""

    async def _submit_job(self) -> int:
        script = self._render_script()

        job_desc = {
            "name": f"jupyter-{self.user.name}",
            "time_limit": self._uint_value(self._parse_time_limit_minutes(self.time_limit)),
        }
        if self.partition:
            job_desc["partition"] = self.partition
        if self.account:
            job_desc["account"] = self.account
        if self.qos:
            job_desc["qos"] = self.qos

        payload: Dict[str, Any] = {
            "job": job_desc,
            "script": script,
        }
        if self.slurm_cluster:
            payload["cluster"] = self.slurm_cluster

        resp = self._to_dict(await self._slurm_call("post_job_submit", payload))
        job_id = int(resp.get("job_id", 0))
        if not job_id:
            raise RuntimeError(f"Slurm REST submit response did not include job_id: {resp}")
        return job_id

    async def _get_job(self, job_id: int) -> Optional[Dict[str, Any]]:
        try:
            resp = self._to_dict(await self._slurm_call("get_job", job_id))
        except RuntimeError as e:
            if "404" in str(e):
                return None
            raise

        jobs = resp.get("jobs") or []
        if not jobs:
            return None
        return jobs[0]

    async def start(self):
        if not self.port:
            self.port = 8888

        self.job_id = await self._submit_job()
        self.log.info("Submitted Slurm job %s for %s", self.job_id, self.user.name)
        started = time.monotonic()

        while True:
            if time.monotonic() - started > self.start_timeout:
                raise TimeoutError(
                    f"Slurm job {self.job_id} did not reach RUNNING within {self.start_timeout}s"
                )

            if await self.poll() is not None:
                raise RuntimeError(f"Slurm job {self.job_id} exited before becoming RUNNING")

            job = await self._get_job(self.job_id)
            state = self._job_state(job or {})
            if state == "RUNNING":
                host = self._job_host(job or {}) or self.execution_host_fallback
                if not host:
                    raise RuntimeError(
                        f"Job {self.job_id} is RUNNING but execution host was not found; "
                        "configure execution_host_fallback"
                    )
                self.log.info("Slurm job %s running on %s:%s", self.job_id, host, self.port)
                return host, self.port

            await asyncio.sleep(self.startup_poll_interval)

    async def poll(self):
        if not self.job_id:
            return 0

        job = await self._get_job(self.job_id)
        if not job:
            return 1

        state = self._job_state(job)
        if state in {"RUNNING", "PENDING", "COMPLETING", "CONFIGURING"}:
            return None

        if state in {"COMPLETED"}:
            return 0

        return 1

    async def stop(self, now=False):
        if not self.job_id:
            return

        await self._slurm_call("delete_job", self.job_id)

    def get_state(self):
        state = super().get_state()
        if self.job_id:
            state["job_id"] = self.job_id
        return state

    def load_state(self, state):
        super().load_state(state)
        self.job_id = int(state.get("job_id", 0))

    def clear_state(self):
        super().clear_state()
        self.job_id = 0
