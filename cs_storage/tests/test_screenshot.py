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


def test_taxbrain_outputs():
    with open(f"{CURRENT_DIR}/test-tb-remote.json") as f:
        remote_outputs = json.loads(f.read())
    outputs = cs_storage.read(remote_outputs["outputs"])

    for output in outputs["renderable"]:
        basename = f"{output['title'] or 'template'}.html"
        print(f"screenshotting: {basename}")
        cs_storage.screenshot(output)
