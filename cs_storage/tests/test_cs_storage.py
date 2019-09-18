import io
import json
import uuid
import zipfile

import pytest
import requests
from marshmallow import exceptions

import cs_storage


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


def test_get_serializer():
    types = ["bokeh", "table", "CSV", "PNG", "JPEG", "MP3", "MP4", "HDF5"]
    for t in types:
        assert cs_storage.get_serializer(t)


def test_cs_storage():
    exp_loc_res = {
        "renderable": [
            {
                "media_type": "bokeh",
                "title": "bokeh plot",
                "data": {"html": "<div/>", "javascript": "console.log('hello world')"},
            },
            {"media_type": "table", "title": "table stuff", "data": "<table/>"},
            {"media_type": "PNG", "title": "PNG data", "data": b"PNG bytes"},
            {"media_type": "JPEG", "title": "JPEG data", "data": b"JPEG bytes"},
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
    task_id = uuid.uuid4()
    rem_res = cs_storage.write(task_id, exp_loc_res)
    loc_res = cs_storage.read(rem_res)
    for output_type in ["renderable", "downloadable"]:
        exp_res = exp_loc_res[output_type]
        loc_res_without_id = [
            {k: v for k, v in output.items() if k != "id"}
            for output in loc_res[output_type]
        ]
        assert exp_res == loc_res_without_id

    loc_res1 = cs_storage.read({"renderable": rem_res["renderable"]})
    exp_res = exp_loc_res["renderable"]
    loc_res_without_id = [
        {k: v for k, v in output.items() if k != "id"}
        for output in loc_res1["renderable"]
    ]
    assert exp_res == loc_res_without_id


def test_errors():
    with pytest.raises(exceptions.ValidationError):
        cs_storage.write("123", {"bad": "data"})
    with pytest.raises(exceptions.ValidationError):
        cs_storage.read({"bad": "data"})
