# SlurmRESTAPISpawner

A custom JupyterHub Spawner that integrates with the Slurm REST API (`slurmrestd`) to manage user servers. This spawner leverages `slurmrestpy` to submit, monitor, and terminate Slurm jobs, providing a seamless experience for users requiring Slurm-based resource management.

## Features

- **Job Submission**: Submits Slurm batch jobs via `POST /slurm/<api>/job/submit`.
- **Job Monitoring**: Polls job states using `GET /slurm/<api>/job/<job_id>`.
- **Job Termination**: Stops jobs through `DELETE /slurm/<api>/job/<job_id>`.
- **State Persistence**: Persists `job_id` in the spawner state to handle Hub restarts.
- **User Options Form**: Provides a customizable spawn form for per-user Slurm options, including:
  - `account`
  - `partition`
  - `qos`
  - `time_limit`
  - `slurm_token`
  - `slurm_user`

## Prerequisites

Before using this spawner, ensure the following:

- A running instance of `slurmrestd` that you can access throught the slurmestd url
- A valid Slurm JWT token for authentication: `scontrol token` to obtain one after connecting to the cluster
- Python 3.10 or higher.
- JupyterHub installed and configured with jupyterlab.

## Installation

Install the spawner and its dependencies using:

```bash
pip install .
```

This will also install `slurmrestpy` as a dependency.

## Configuration

Add the following configuration to your JupyterHub `jupyterhub_config.py` file:

```python
c.JupyterHub.spawner_class = "slurmrestapispawner.SlurmRESTAPISpawner"

c.SlurmRESTAPISpawner.slurmrestd_url = "https://slurmrestd.example.org:6820"
c.SlurmRESTAPISpawner.slurm_api_version = "v0.0.40"
c.SlurmRESTAPISpawner.slurm_token = "SLURM JWT token"
c.SlurmRESTAPISpawner.user_options_form = True
c.SlurmRESTAPISpawner.partition = "interactive"
c.SlurmRESTAPISpawner.slurm_user = "my-user"
c.SlurmRESTAPISpawner.time_limit = "02:00:00"
```

## Usage

1. Start JupyterHub with the configured spawner.
2. Users will see a spawn form to customize their Slurm job options.
3. The spawner will handle job submission, monitoring, and termination transparently.

## Notes

- The spawner uses the following `slurmrestpy` endpoints:
  - `slurm_vXXXX_post_job_submit`
  - `slurm_vXXXX_get_job`
  - `slurm_vXXXX_delete_job`
- Ensure that the `slurm_api_version` matches the supported API versions of your Slurm installation.

## Contributing

Contributions are welcome! If you encounter issues or have feature requests, please open an issue or submit a pull request.

## License

This project is licensed under the MIT License. See the LICENSE file for details.

