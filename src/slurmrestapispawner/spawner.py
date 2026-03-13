import re
import shlex
import time
import asyncio
import math
from typing import Any, Dict, Optional

from jupyterhub.spawner import Spawner
from jupyterhub.services.auth import HubAuth
from traitlets import Bool, Dict as DictTrait, Int, Unicode, List, Integer


class SlurmRESTAPISpawner(Spawner):
    """Spawn single-user servers by submitting jobs to slurmrestd.

    Flow:
    1. Submit job via slurmrestd.
    2. Poll job state until RUNNING.
    3. Determine execution host from the job info.
    4. Return (ip, port) for JupyterHub to connect to.
    """

    # Base URL for slurmrestd, e.g. "https://slurm.example.com:6820". This is used to construct the full API endpoint URLs for job submission and management.
    slurmrestd_url = Unicode(
        "",
        config=True,
        help="Base URL for slurmrestd.",
    )

    # The slurm_api_version is used to determine which openapi_client method to call for each operation.
    # It should match the version of slurmrestd you are using, and the openapi_client library should have generated methods for that version.
    # The _resolve_method_name function uses this version to find the appropriate method in the openapi_client.SlurmApi class, allowing compatibility with multiple API versions.
    # Adjust this version as needed when upgrading slurmrestd or openapi_client.
    slurm_api_version = Unicode(
        "v0.0.40",
        config=True,
        help="Slurm REST API version segment used in endpoint URLs.",
    )

    # If your slurmrestd setup uses clusters or partitions that require specifying a cluster name in the API calls, you can set it here.
    # It will be included in the job submission payload to slurmrestd. If not needed, leave it empty.
    slurm_cluster = Unicode(
        "",
        config=True,
        help="Optional cluster name for job submission.",
    )


    # The slurm_token is required for authenticating with slurmrestd.
    # It can be set via configuration or provided by the user in the spawn form. 
    # The token must have appropriate permissions to submit and manage jobs through slurmrestd.
    slurm_token = Unicode(
        "",
        config=True,
        help="Slurm REST auth token.",
    )

    # The slurm_user is the username that will be used for authenticating with slurmrestd.
    # It can be set via configuration or provided by the user in the spawn form. If not set, it defaults to the JupyterHub username.
    # This allows for flexibility in cases where the Slurm username differs from the JupyterHub username, or when a shared service account is used for job submission. 
    slurm_user = Unicode(
        "",
        config=True,
        help="Slurm user override for REST requests. If empty, JupyterHub username is used.",
    )

    # Whether to validate TLS certificates for slurmrestd HTTPS calls.
    validate_cert = Bool(
        True,
        config=True,
        help="Validate TLS certificates for slurmrestd HTTPS calls.",
    )

    # Per-request timeout for Slurm REST API calls. Adjust as needed based on your cluster's typical response times and network conditions.
    # A reasonable default is 30 seconds, but you may want to increase it if you have a large cluster or if slurmrestd is known to respond slowly.
    request_timeout = Int(
        30,
        config=True,
        help="Per-request timeout (seconds) for Slurm REST calls.",
    )

    # If True, log detailed information about Slurm REST API requests and responses, with sensitive info redacted.
    # Useful for debugging API interactions and payloads, but can expose sensitive info and should be used with caution.
    debug_slurm_api = Bool(
        False,
        config=True,
        help="Enable verbose debug logging of Slurm REST requests/responses (sanitized).",
    )

    # If True, log the full rendered batch script before submission. Useful for debugging script generation issues, but can expose sensitive info and should be used with caution. 
    debug_show_batch_script = Bool(
        False,
        config=True,
        help="If True, log the full rendered batch script before job submission.",
    )

    # Interval between polling Slurm for job state during startup. Adjust as needed for your cluster's typical job start times and slurmrestd performance.
    # A shorter interval can lead to faster detection of the job starting, but may increase load on slurmrestd and the cluster if set too low.
    startup_poll_interval = Int(
        3,
        config=True,
        help="Seconds between Slurm job state checks during startup.",
    )

    # Job defaults
    partition = Unicode("", config=True, help="Slurm partition.")

    # Account is optional and can be left empty if not used in your cluster. If specified, it will be included in the job submission payload to slurmrestd. i
    account = Unicode("", config=True, help="Slurm account.")

    # The slurm_token can be set via config or provided by the user in the spawn form. It is used for authenticating with slurmrestd and must have appropriate permissions to submit and manage jobs.i
    slurm_token = Unicode(
        "",
        config=True,
        help="Slurm token",
    )

    # QoS is optional and can be left empty if not used in your cluster. If specified, it will be included in the job submission payload to slurmrestd.
    qos = Unicode("", config=True, help="Slurm QoS.")
    # Slurm time limit can be specified in minutes (e.g. "120") or in the format [[DD-]HH:]MM:SS (e.g. "02:00:00" or "1-00:00:00"). i
    # The _parse_time_limit_minutes method converts these formats to total minutes for the Slurm API.
    time_limit = Unicode("01:00:00", config=True, help="Slurm time limit HH:MM:SS.")

    # If True, show a spawn form allowing users to override Slurm job options.
    enable_user_options_form = Bool(
        True,
        config=True,
        help="If True, show a spawn form allowing users to override Slurm job options.",
    )

    # The job_id is stored in the spawner state to track the submitted Slurm job across restarts and for polling its status. It is not set until start() is called and a job is submitted.
    job_id = Unicode("")

    # The prologue is a set of shell commands that run before the single-user server starts. i
    # This is where you can load modules, activate virtual environments, and set up the environment for the Jupyter server. 
    # The example prologue loads Python and activates a virtual environment, but you can customize it as needed for your cluster setup.
    prologue = Unicode("",
        config=True,
        help="Prologue commands to run before the single-user server starts.",
    )

    # The epilogue can be used for cleanup commands or diagnostics after the single-user server starts. It runs in the same job script, so it can access the same environment variables and context.
    epilogue = Unicode(
        "",
        config=True,
        help="Epilogue commands to run after the single-user server starts.",
    )

    # The batch script template. The prologue, singleuser_cmd, and epilogue are substituted into this template to create the final script that is submitted to Slurm. 
    batch_script = Unicode(
        "#!/bin/bash -l\n\n{prologue}\n\n{singleuser_cmd}\n\n{epilogue}",
        config=False,
        help=(
            "Batch script template rendered with: prologue, singleuser_cmd, epilogue. "
            "singleuser_cmd already includes proper shell quoting."
        ),
    )

    # The wrapper_cmd is used to invoke the slurmrestapi-singleuser script, which captures the environment variables and other context for the single-user server.
    wrapper_cmd = Unicode(
        "slurmrestapi-singleuser",
        config=True,
        help="Command name for the slurmrestapi-singleuser wrapper script.",
    )

    # The current_working_directory is set in the job submission payload to specify the working directory for the single-user server. 
    # It defaults to empty, which means slurmrestd will use its default behavior (often the home directory of the user).
    # You can set this to a specific path if needed for your cluster setup.
    current_working_directory = Unicode(
        "",
        config=True,
        help="Current working directory for the single-user server.",
    )

    def options_form(self, spawner=None):
        """
        If enable_user_options_form is True, show form fields for Slurm job options.
        """
        s = spawner or self
        if not s.enable_user_options_form:
            return ""
        return f"""
<div style="max-width: 400px; margin: auto;">
    <div style="margin-bottom: 1em;">
        <label for="slurm-account">Account</label>
        <input name="account" id="slurm-account" type="text" value="{s.account}" placeholder="Enter account" style="width: 100%;" />
    </div>
    <div style="margin-bottom: 1em;">
        <label for="slurm-partition">Partition</label>
        <input name="partition" id="slurm-partition" type="text" value="{s.partition}" placeholder="Enter partition" style="width: 100%;" />
    </div>
    <div style="margin-bottom: 1em;">
        <label for="slurm-qos">QoS</label>
        <input name="qos" id="slurm-qos" type="text" value="{s.qos}" placeholder="Enter QoS" style="width: 100%;" />
    </div>
    <div style="margin-bottom: 1em;">
        <label for="slurm-time-limit">Time Limit</label>
        <input name="time_limit" id="slurm-time-limit" type="text" value="{s.time_limit}" placeholder="e.g., 02:00:00" style="width: 100%;" />
    </div>
    <div style="margin-bottom: 1em;">
        <label for="slurm-token">Token</label>
        <input name="token" id="slurm-token" type="password" value="{s.slurm_token}" placeholder="SLURM JWT token" style="width: 100%;" />
    </div>
    <div style="margin-bottom: 1em;">
        <label for="slurm-user">Slurm User</label>
        <input name="slurm_user" id="slurm-user" type="text" value="{s.slurm_user}" placeholder="Enter Slurm user" style="width: 100%;" />
    </div>
    <div style="margin-bottom: 1em;">
        <label for="cwd">Current Working Directory</label>
        <input name="current_working_directory" id="cwd" type="text" value="{s.current_working_directory}" placeholder="Enter working directory" style="width: 100%;" />
    </div>
</div>
"""

    def options_from_form(self, formdata):
        """
            Parse the submitted form data for Slurm job options. Each key is expected to be a list of values, so we take the first one.
        """
        # JupyterHub sends each key as a list of submitted values.
        parsed = {}
        for key in ("account", "partition", "qos", "time_limit", "token", "slurm_user","current_working_directory"):
            value = formdata.get(key, [""])
            if isinstance(value, list):
                value = value[0] if value else ""
            parsed[key] = str(value).strip()
        if self.debug_slurm_api:
            self.log.warning("Spawn form received raw=%s parsed=%s", formdata, parsed)
        return parsed

    async def apply_user_options(self, spawner, user_options):
        """
        Apply user options from the spawn form to the spawner's attributes for job submission.
        """
        s = spawner or self
        if not isinstance(user_options, dict):
            return

        if s.debug_slurm_api:
            s.log.warning("Applying user options: %s", user_options)

        for key in ("account", "partition", "qos", "time_limit", "current_working_directory"):
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
        """
        Determine the Slurm user to use for REST requests. The precedence is:
        1. User provided via spawn form. 
        2. User set via configuration.   
        """
        if self.slurm_user=="":
            raise ValueError("slurm_user must be set either via config or spawn form")  
        return self.slurm_user

    def _slurm_token_value(self) -> str:
        """
        Determine the Slurm REST auth token to use. The precedence is:
        1. Token provided via spawn form.
        2. Token set via configuration.
        """
        if self.slurm_token=="":
            raise ValueError("slurm_token must be set either via config or spawn form")  
        return  self.slurm_token

    def _api_version_suffix(self) -> str:
        """
        Convert slurm_api_version like "v0.0.40" to a suffix like "v0040" used in openapi_client method names. 
        This allows using newer API versions with older openapi_client libraries that may not have explicit support for them, as long as the method signatures are compatible.
        """
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
                f"Unsupported major version in slurm_api_version '{
                    self.slurm_api_version
                }'. "
                "Expected v0.x.y format."
            )
        return f"v{minor:02d}{patch:02d}"

    def _resolve_method_name(self, api: Any, op: str) -> str:
        """
        Resolve the appropriate method name for a given operation based on the API version.
        """
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
            raise RuntimeError(
                f"openapi_client does not expose any method for operation '{op}'"
            )

        available.sort(reverse=True)
        fallback_name = available[0][1]
        self.log.warning(
            "openapi_client does not expose method '%s'; falling back to '%s'. "
            "Adjust slurm_api_version or upgrade openapi_client.",
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

    def _method_suffix_to_api_version(self, method_name: str) -> str:
        m = re.match(r"^slurm_v(\d{2})(\d{2})_", method_name)
        if not m:
            return self.slurm_api_version
        return f"v0.{int(m.group(1))}.{int(m.group(2))}"

    def _api_path_for_op(self, op: str, args: Any) -> str:
        if op == "post_job_submit":
            return "/job/submit"
        if op == "get_job" and args:
            return f"/job/{args[0]}"
        if op == "delete_job" and args:
            return f"/job/{args[0]}"
        return f"/{op}"

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
                f"Unsupported time_limit format '{
                    value
                }'. Expected minutes or [[DD-]HH:]MM:SS"
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

    async def _slurm_call(self, op: str, *args):
        try:
            import openapi_client
        except Exception as e:
            raise RuntimeError("openapi_client is required but not installed.") from e

        def _run():
            cfg = openapi_client.Configuration(host=self.slurmrestd_url.rstrip("/"))
            cfg.verify_ssl = self.validate_cert

            cfg.username = self._slurm_user_value()
            cfg.access_token = self._slurm_token_value()
            with openapi_client.ApiClient(cfg) as api_client:
                api = openapi_client.SlurmApi(api_client)
                method_name = self._resolve_method_name(api, op)
                method = getattr(api, method_name)
                result = method(*args, _request_timeout=self.request_timeout)
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
                raise RuntimeError(
                    f"Slurm REST request failed ({status}): {reason} {body}"
                ) from e
            raise

    def _singleuser_command(self) -> str:
        """
        Construct the command to launch the single-user server, wrapped by the slurmrestapi-singleuser script for environment capture.
        """
        wrapper_cmd="slurmrestapi-singleuser"
        argv = [f"{self.wrapper_cmd}"] + list(self.cmd) + list(self.get_args())
        argv.extend(["--ip=0.0.0.0"])
        return shlex.join(argv)

    def _render_script(self) -> str:
        """
        Render the batch script by substituting the prologue, singleuser_cmd, and epilogue into the batch_script template.
        """
        return self.batch_script.format(
            prologue=self.prologue,
            singleuser_cmd=self._singleuser_command(),
            epilogue=self.epilogue,
        )

    def _job_state(self, job: Dict[str, Any]) -> str:
        """
        Extract the job state from the Slurm job info dictionary. The state is returned as a list of strings (e.g. ["RUNNING", "COMPLETED"]).
        """
        return list(map(lambda x: x.upper(), job.get("job_state", [])))

    async def _submit_job(self) -> str:
        """
        Submit a job to slurmrestd with the rendered batch script and return the job ID.
        """
        script = self._render_script()
        if self.debug_show_batch_script:
            self.log.warning(
                "Rendered Slurm batch script for %s:\n%s", self.user.name, script
            )

        

        job_desc = {
            "name": f"jupyter-{self.slurm_user}",
            "time_limit": self._uint_value(
                self._parse_time_limit_minutes(self.time_limit)
            ),
            "current_working_directory": self.current_working_directory,
            # Like for the batchspawner, transfer all env variables
            "environment": [f"{k}={v}" for k, v in self.get_env().items()] + [f"PATH=/bin/:/usr/bin/:/sbin/"],
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
        job_id = str(resp.get("job_id", ""))
        if not job_id:
            raise RuntimeError(
                f"Slurm REST submit response did not include job_id: {resp}"
            )
        return job_id

    async def _get_job(self, job_id: str) -> Optional[Dict[str, Any]]:
        """
        Get job info from slurmrestd for the given job ID. If the job is not found (e.g. 404), return None. For other errors, raise an exception.
        """

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
        """
            Start the single-user server by submitting a Slurm job and waiting for it to start running.
            1. Submit job via slurmrestd and get job ID.
            2. Poll job state until RUNNING.
            3. Once running, return (ip, port) for JupyterHub to connect to.
        """

        self.job_id = await self._submit_job()
        if len(self.job_id) == 0:
            raise RuntimeError(
                "Jupyter job submission failure (no jobid in output)"
           )
        self.log.info("Submitted Slurm job %s for %s", self.job_id, self.user.name)

        while True:
            job = await self._get_job(self.job_id)
            state = self._job_state(job)
            match state[0]:
                case "RUNNING":
                    self.log.info("Slurm job %s for %s is running", self.job_id, self.user.name)
                    break
                case "PENDING" | "CONFIGURING":
                    self.log.info("Slurm job %s for %s is pending", self.job_id, self.user.name)
                case "COMPLETED":
                    raise RuntimeError(f"Slurm job {self.job_id} completed without starting")

            await asyncio.sleep(self.startup_poll_interval)
        while self.port==0:
            self.log.info("Waiting or singleuser server to bind to start")
            await asyncio.sleep(self.startup_poll_interval)
        self.db.commit()
        self.log.info(
            "Notebook server job {} started at {}:{}".format(
                self.job_id, self.ip, self.port
            )
        )

        return (self.ip, self.port)


    async def cancel_job(self):
        """
        Cancel the currently running or pending Slurm job.
        """
        if not self.job_id:
            raise RuntimeError("No job_id found. Cannot cancel a job that hasn't been submitted.")

        try:
            await self._slurm_call("delete_job", self.job_id)
            self.log.info(f"Successfully cancelled job {self.job_id} for user {self.user.name}.")
        except Exception as e:
            self.log.error(f"Failed to cancel job {self.job_id} for user {self.user.name}: {e}")
            raise

    async def poll(self):
        """
        Poll the status of the Slurm job to determine if the single-user server is running, pending, or has completed/failed.

        Returns:
            - None: If the job is running or pending.
            - 0: If the job has completed successfully.
            - 1: If the job has failed, completed with errors, or is not found.
        """
        if not self.job_id:
            self.log.warning("No job_id found. Returning failed status.")
            return 1

        try:
            job = await self._get_job(self.job_id)
        except Exception as e:
            self.log.error(f"Failed to fetch job details for job_id {self.job_id}: {e}")
            return 1

        if not job:
            self.log.warning(f"Job with job_id {self.job_id} not found. Returning failed status.")
            return 1

        state = self._job_state(job)
        if not state:
            self.log.warning(f"Job state for job_id {self.job_id} is empty. Returning failed status.")
            return 1

        self.log.info(f"Job {self.job_id} is in state: {state[0]}")
        match state[0]:
            case "RUNNING" | "PENDING" | "COMPLETING" | "CONFIGURING":
                return None
            case "COMPLETED":
                return 0
            case _:
                self.log.warning(f"Job {self.job_id} is in an unexpected state: {state[0]}. Returning failed status.")
                return 1

    async def stop(self,now=False):
        """
        Stop the single-user server by canceling the Slurm job via slurmrestd.
        """
        if not self.job_id:
            return
        await self._slurm_call("delete_job", self.job_id)

    async def progress(self):
        job = await self._get_job(self.job_id)
        if not job:
            yield {"message": "Job not found..."}
            return
        state = self._job_state(job)
        while True:
            match state[0]:
                case "PENDING" | "CONFIGURING":
                    yield {"message": f"Cluster job {self.job_id} is pending in queue..."}
                case "RUNNING":
                    yield {"message": f"Cluster job {self.job_id} is running ... waiting to connect "}
                    return
                case "FAILED":
                    yield {"message": f"{self.job_id} failed ..."}
                case _:
                    yield {"message": f"{self.job_id} has an unknown state ..."}
            await asyncio.sleep(1)

    def get_state(self):
        """
        Get the spawner state to be stored by JupyterHub. We include the job_id so that we can track the Slurm job across restarts and for polling its status.
        """
        state = super().get_state()
        if self.job_id:
            state["job_id"] = self.job_id
        return state

    def load_state(self, state):
        """
        Load the spawner state from the stored state. We extract the job_id to continue tracking the Slurm job across restarts and for polling its status.
        """
        super().load_state(state)
        self.job_id = str(state.get("job_id", ""))

    def clear_state(self):
        """
        Clear the spawner state. We also clear the job_id to ensure that we don't accidentally track an old job after a stop or if the state is cleared for any reason.
        """
        super().clear_state()
        self.job_id = ""
