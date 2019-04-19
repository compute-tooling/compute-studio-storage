import io
import json
import uuid
import zipfile

import pytest
import requests
from marshmallow import exceptions

import s3like


def test_JSONSerializer():
    ser = s3like.JSONSerializer("json")

    act = ser.serialize({"hello": "world"})
    assert isinstance(act, bytes)
    assert json.loads(act.decode()) == {"hello": "world"}

    act = ser.deserialize(b'{"hello": "world"}')
    assert isinstance(act, dict)
    assert act == {"hello": "world"}


def test_text_serializer():
    ser = s3like.TextSerializer("txt")

    act = ser.serialize("hello world")
    assert isinstance(act, bytes)
    assert act.decode() == "hello world"

    act = ser.deserialize(b"hello world")
    assert isinstance(act, str)
    assert act == "hello world"


def test_serializer():
    ser = s3like.Serializer("txt")

    act = ser.serialize(b"hello world")
    assert isinstance(act, bytes)
    assert act == b"hello world"

    act = ser.deserialize(b"hello world")
    assert isinstance(act, bytes)
    assert act == b"hello world"


def test_get_serializer():
    types = ["bokeh", "table", "CSV", "PNG", "JPEG", "MP3", "MP4", "HDF5"]
    for t in types:
        assert s3like.get_serializer(t)


def test_s3like():
    exp_loc_res = {
        "renderable": [
            {
                "media_type": "bokeh",
                "title": "bokeh plot",
                "data": {"html": "<div/>", "javascript": "console.log('hello world')"},
            },
            {
                "media_type": "table",
                "title": "table stuff",
                "data": "<table/>",
            },
            {
                "media_type": "PNG",
                "title": "PNG data",
                "data": b"PNG bytes",
            },
            {
                "media_type": "JPEG",
                "title": "JPEG data",
                "data": b"JPEG bytes",
            },
            {
                "media_type": "MP3",
                "title": "MP3 data",
                "data": b"MP3 bytes",
            },

            {
                "media_type": "MP4",
                "title": "MP4 data",
                "data": b"MP4 bytes",
            },
        ],
        "downloadable": [
            {
                "media_type": "CSV",
                "title": "CSV file",
                "data": "comma,sep,values\n"
            },
            {
                "media_type": "HDF5",
                "title": "HDF5 file",
                "data": b"serialized numpy arrays and such\n"
            },
        ],
    }
    task_id = uuid.uuid4()
    rem_res = s3like.write_to_s3like(task_id, exp_loc_res)
    loc_res = s3like.read_from_s3like(rem_res)
    assert loc_res == exp_loc_res

    loc_res1 = s3like.read_from_s3like({"renderable": rem_res["renderable"]})
    assert loc_res1["renderable"] == exp_loc_res["renderable"]


def test_errors():
    with pytest.raises(exceptions.ValidationError):
        s3like.write_to_s3like("123", {"bad": "data"})
    with pytest.raises(exceptions.ValidationError):
        s3like.read_from_s3like({"bad": "data"})
