import base64
import io
import json
import uuid
import zipfile

import pytest
import requests
from marshmallow import exceptions

import cs_storage


@pytest.fixture
def png():
    import matplotlib.pyplot as plt
    import numpy as np

    x = np.linspace(0, 2, 100)
    plt.figure()
    plt.plot(x, x, label="linear")
    plt.plot(x, x ** 2, label="quadratic")
    plt.plot(x, x ** 3, label="cubic")
    plt.xlabel("x label")
    plt.ylabel("y label")
    plt.title("Simple Plot")
    plt.legend()
    initial_buff = io.BytesIO()
    plt.savefig(initial_buff, format="png")
    initial_buff.seek(0)
    return initial_buff.read()


@pytest.fixture
def jpg():
    import matplotlib.pyplot as plt
    import numpy as np

    x = np.linspace(0, 2, 100)
    plt.figure()
    plt.plot(x, x, label="linear")
    plt.plot(x, x ** 2, label="quadratic")
    plt.plot(x, x ** 3, label="cubic")
    plt.xlabel("x label")
    plt.ylabel("y label")
    plt.title("Simple Plot")
    plt.legend()
    initial_buff = io.BytesIO()
    plt.savefig(initial_buff, format="jpg")
    initial_buff.seek(0)
    return initial_buff.read()


def test_JSONSerializer():
    ser = cs_storage.JSONSerializer("json")

    act = ser.serialize({"hello": "world"})
    assert isinstance(act, bytes)
    assert json.loads(act.decode()) == {"hello": "world"}

    act = ser.deserialize(b'{"hello": "world"}')
    assert isinstance(act, dict)
    assert act == {"hello": "world"}


def test_text_serializer():
    ser = cs_storage.TextSerializer("txt")

    act = ser.serialize("hello world")
    assert isinstance(act, bytes)
    assert act.decode() == "hello world"

    act = ser.deserialize(b"hello world")
    assert isinstance(act, str)
    assert act == "hello world"


def test_serializer():
    ser = cs_storage.Serializer("txt")

    act = ser.serialize(b"hello world")
    assert isinstance(act, bytes)
    assert act == b"hello world"

    act = ser.deserialize(b"hello world")
    assert isinstance(act, bytes)
    assert act == b"hello world"


def test_base64serializer(png, jpg):
    """Test round trip serializtion/deserialization of PNG and JPG"""
    ser = cs_storage.Base64Serializer("PNG")
    asbytes = ser.serialize(png)
    asstr = ser.deserialize(asbytes)
    assert png == ser.from_string(asstr)
    assert json.dumps({"pic": asstr})

    ser = cs_storage.Base64Serializer("JPG")
    asbytes = ser.serialize(jpg)
    asstr = ser.deserialize(asbytes)
    assert jpg == ser.from_string(asstr)
    assert json.dumps({"pic": asstr})


def test_get_serializer():
    types = ["bokeh", "table", "CSV", "PNG", "JPEG", "MP3", "MP4", "HDF5"]
    for t in types:
        assert cs_storage.get_serializer(t)


def test_cs_storage(png, jpg):
    dummy_uuid = "c7a65ad2-0c2c-45d7-b0f7-d9fd524c49b3"
    exp_loc_res = {
        "renderable": [
            {
                "media_type": "bokeh",
                "title": "bokeh plot",
                "data": {"html": "<div/>", "javascript": "console.log('hello world')"},
            },
            {"media_type": "table", "title": "table stuff", "data": "<table/>"},
            {"media_type": "PNG", "title": "PNG data", "data": png},
            {"media_type": "JPEG", "title": "JPEG data", "data": jpg},
            {"media_type": "MP3", "title": "MP3 data", "data": b"MP3 bytes"},
            {"media_type": "MP4", "title": "MP4 data", "data": b"MP4 bytes"},
        ],
        "downloadable": [
            {"media_type": "CSV", "title": "CSV file", "data": "comma,sep,values\n"},
            {
                "media_type": "HDF5",
                "title": "HDF5 file",
                "data": b"serialized numpy arrays and such\n",
            },
            {"media_type": "PDF", "title": "PDF file", "data": b"some pdf like data."},
            {
                "media_type": "Markdown",
                "title": "Markdown file",
                "data": "**hello world**",
            },
            {"media_type": "Text", "title": "Text file", "data": "text data"},
        ],
    }
    task_id = "1868c4a7-b03c-4fe4-ab45-0aa95c0bfa53"
    rem_res = cs_storage.write(task_id, exp_loc_res)
    loc_res = cs_storage.read(rem_res, json_serializable=False)
    for output_type in ["renderable", "downloadable"]:
        loc_res_without_id = [
            {k: v for k, v in output.items() if k != "id"}
            for output in loc_res[output_type]
        ]
        exp_res_without_id = [
            {k: v for k, v in output.items() if k != "id"}
            for output in exp_loc_res[output_type]
        ]
        assert exp_res_without_id == loc_res_without_id

    assert json.dumps(cs_storage.read(rem_res, json_serializable=True))

    loc_res1 = cs_storage.read(
        {"renderable": rem_res["renderable"]}, json_serializable=False
    )
    loc_res_without_id = [
        {k: v for k, v in output.items() if k != "id"}
        for output in loc_res1["renderable"]
    ]
    exp_res_without_id = [
        {k: v for k, v in output.items() if k != "id"}
        for output in exp_loc_res["renderable"]
    ]

    assert exp_res_without_id == loc_res_without_id
    assert json.dumps(
        cs_storage.read({"renderable": rem_res["renderable"]}, json_serializable=True)
    )


def test_add_screenshot_links():
    rem_res = {"renderable": {"outputs": [{"id": "1234"}, {"id": "4567"}]}}

    url = f"https://storage.googleapis.com/{cs_storage.BUCKET}/"
    assert cs_storage.add_screenshot_links(rem_res) == {
        "renderable": {
            "outputs": [
                {"id": "1234", "screenshot": url + "1234.png"},
                {"id": "4567", "screenshot": url + "4567.png"},
            ]
        }
    }


def test_errors():
    with pytest.raises(exceptions.ValidationError):
        cs_storage.write("123", {"bad": "data"})
    with pytest.raises(exceptions.ValidationError):
        cs_storage.read({"bad": "data"})
