import json
import os

import cs_storage


CURRENT_DIR = os.path.abspath(os.path.dirname(__file__))


def test_taxcruncher_outputs():
    with open(f"{CURRENT_DIR}/test-tc-remote.json") as f:
        remote_outputs = json.loads(f.read())
    outputs = cs_storage.read(remote_outputs["outputs"])

    for output in outputs["renderable"]:
        basename = f"{output['title'] or 'template'}.html"
        print(f"screenshotting: {basename}")
        cs_storage.screenshot(output)


# def test_taxbrain_outputs():
#     with open(f"{CURRENT_DIR}/test-tb-remote.json") as f:
#         remote_outputs = json.loads(f.read())
#     outputs = cs_storage.read(remote_outputs["outputs"])

#     for output in outputs["renderable"]:
#         basename = f"{output['title'] or 'template'}.html"
#         print(f"screenshotting: {basename}")
#         cs_storage.screenshot(output)


def test_use_with_dask():
    try:
        import dask
        import dask.distributed
        from distributed import Client
    except ImportError:
        import warnings

        warnings.warn("Dask and/or Distributed are not installed")
        return
    with open(f"{CURRENT_DIR}/test-ogusa-remote.json") as f:
        remote_outputs = json.loads(f.read())
    outputs = cs_storage.read(remote_outputs["outputs"])

    c = Client()
    futures = c.map(cs_storage.screenshot, outputs["renderable"])
    results = c.gather(futures)
    for result in results:
        assert isinstance(result, bytes)
