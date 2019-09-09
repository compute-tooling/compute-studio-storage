# Compute Studio Storage

A light-weight package that is used by [Compute Studio](https://compute.studio) to read and write model results to Google Cloud Storage.

## Setup

```bash
pip install cs-storage
export BUCKET=YOUR_BUCKET
```

## Authenticate

```bash
gcloud auth login
gcloud auth application-default login
```

## Use

```python
import cs_storage

# run_model returns data that is compliant with the C/S outputs api.
local_result, task_id = run_model(**kwargs)
remote_result = cs_storage.write(task_id, local_result)
round_trip = cs_storage.read(remote_result)
assert local_result == round_trip
```

## Test

```bash
py.test -v
```
