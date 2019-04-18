# s3like

A small package that is used by COMP to read and write model results to S3 like object storage systems. This means that this package is compatible with DigitalOcean Spaces and AWS S3, since DO Spaces uses the same API as AWS S3.

Setup:
-------------------

```bash
pip install s3like
export OBJ_STORAGE_ACCESS=...
export OBJ_STORAGE_SECRET=...
export OBJ_STORAGE_ENDPOINT=...
export OBJ_STORAGE_EDGE=...
export OBJ_STORAGE_BUCKET=...
```

Use:
------------

```python
import s3like

# run_model returns data that is compliant with the COMP outputs api.
local_result, task_id = run_model(**kwargs)
remote_result = s3like.write_to_s3like(task_id, local_result)
round_trip = s3like.read_from_s3like(remote_result)
assert local_result == round_trip
```

Test:
-------------
```bash
py.test -v
```
