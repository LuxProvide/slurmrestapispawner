# slurmrestapiSpawner

A custom JupyterHub Spawner that submits and manages user servers through the Slurm REST API (`slurmrestd`) using `slurmrestpy`.

## What it does

- Submits a Slurm batch job through `POST /slurm/<api>/job/submit`
- Polls Slurm job state through `GET /slurm/<api>/job/<job_id>`
- Stops jobs through `DELETE /slurm/<api>/job/<job_id>`
- Persists `job_id` in spawner state for Hub restarts
- Provides a spawn form for per-user Slurm options (`account`, `partition`, `qos`, `time_limit`, `cpus_per_task`, `mem_per_node`)

## Install

```bash
pip install -e .
```

This installs `slurmrestpy` as a dependency.

## JupyterHub config example

```python
c.JupyterHub.spawner_class = "slurmrestapispawner.SlurmRESTAPISpawner"

c.SlurmRESTAPISpawner.slurmrestd_url = "https://slurmrestd.example.org:6820"
c.SlurmRESTAPISpawner.slurm_api_version = "v0.0.43"
c.SlurmRESTAPISpawner.token_env_var = "SLURM_JWT"
c.SlurmRESTAPISpawner.enable_user_options_form = True

# Optional
c.SlurmRESTAPISpawner.partition = "interactive"
c.SlurmRESTAPISpawner.account = "my-account"
c.SlurmRESTAPISpawner.time_limit = "02:00:00"
c.SlurmRESTAPISpawner.mem_per_node = "8G"
c.SlurmRESTAPISpawner.cpus_per_task = 2
```

## Notes

- The spawner calls the generated `slurmrestpy` endpoints:
  - `slurm_vXXXX_post_job_submit`
  - `slurm_vXXXX_get_job`
  - `slurm_vXXXX_delete_job`
- `slurm_api_version` must match the installed client's supported API versions.
```

