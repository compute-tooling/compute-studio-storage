import base64
import io
import json
import os
import uuid
import zipfile
import time
from collections import namedtuple

import gcsfs
from marshmallow import Schema, fields, validate


__version__ = "1.7.0"


BUCKET = os.environ.get("BUCKET", None)


class Serializer:
    """
    Base class for serializng input data to bytes and back.
    """
    def __init__(self, ext):
        self.ext = ext

    def serialize(self, data):
        return data

    def deserialize(self, data, json_serializable=True):
        return data


class JSONSerializer(Serializer):
    def serialize(self, data):
        return json.dumps(data).encode()

    def deserialize(self, data, json_serializable=True):
        return json.loads(data.decode())


class TextSerializer(Serializer):
    def serialize(self, data):
        return data.encode()

    def deserialize(self, data, json_serializable=True):
        return data.decode()


class Base64Serializer(Serializer):
    def deserialize(self, data, json_serializable=True):
        if json_serializable:
            return base64.b64encode(data).decode("utf-8")
        else:
            return data

    def from_string(self, data):
        return base64.b64decode(data.encode("utf-8"))


def get_serializer(media_type):
    return {
        "bokeh": JSONSerializer("json"),
        "table": TextSerializer("html"),
        "CSV": TextSerializer("csv"),
        "PNG": Base64Serializer("png"),
        "JPEG": Base64Serializer("jpeg"),
        "MP3": Base64Serializer("mp3"),
        "MP4": Base64Serializer("mp4"),
        "HDF5": Base64Serializer("h5"),
        "PDF": Base64Serializer("pdf"),
        "Markdown": TextSerializer("md"),
        "Text": TextSerializer("txt"),
    }[media_type]


class Output:
    """Output mixin shared among LocalOutput and RemoteOutput"""

    title = fields.Str()
    media_type = fields.Str(
        validate=validate.OneOf(choices=["bokeh", "table", "CSV", "PNG", "JPEG", "MP3", "MP4", "HDF5", "PDF", "Markdown", "Text"])
    )


class RemoteOutput(Output, Schema):
    filename = fields.Str()


class RemoteOutputCategory(Schema):
    outputs = fields.Nested(RemoteOutput, many=True)
    ziplocation = fields.Str()


class RemoteResult(Schema):
    """Serializer for read"""

    renderable = fields.Nested(RemoteOutputCategory, required=False)
    downloadable = fields.Nested(RemoteOutputCategory, required=False)


class LocalOutput(Output, Schema):
    # Data could be a string or dict. It depends on the media type.
    data = fields.Field()


class LocalResult(Schema):
    """Serializer for read"""

    renderable = fields.Nested(LocalOutput, many=True)
    downloadable = fields.Nested(LocalOutput, many=True)


def write(task_id, loc_result, do_upload=True):
    fs = gcsfs.GCSFileSystem()
    s = time.time()
    LocalResult().load(loc_result)
    rem_result = {}
    for category in ["renderable", "downloadable"]:
        buff = io.BytesIO()
        zipfileobj = zipfile.ZipFile(buff, mode="w")
        ziplocation = f"{task_id}_{category}.zip"
        rem_result[category] = {"ziplocation": ziplocation, "outputs": []}
        for output in loc_result[category]:
            serializer = get_serializer(output["media_type"])
            ser = serializer.serialize(output["data"])
            filename = output["title"]
            if not filename.endswith(f".{serializer.ext}"):
                filename += f".{serializer.ext}"
            zipfileobj.writestr(filename, ser)
            rem_result[category]["outputs"].append(
                {
                    "title": output["title"],
                    "media_type": output["media_type"],
                    "filename": filename,
                }
            )
        zipfileobj.close()
        buff.seek(0)
        if do_upload:
            with fs.open(f"{BUCKET}/{ziplocation}", "wb") as f:
                f.write(buff.read())
    f = time.time()
    print(f"Write finished in {f-s}s")
    return rem_result


def read(rem_result, json_serializable=True):
    # compute studio results have public read access.
    fs = gcsfs.GCSFileSystem(token="anon")
    s = time.time()
    RemoteResult().load(rem_result)
    read = {"renderable": [], "downloadable": []}
    for category in rem_result:
        with fs.open(f"{BUCKET}/{rem_result[category]['ziplocation']}", "rb") as f:
            res = f.read()

        buff = io.BytesIO(res)
        zipfileobj = zipfile.ZipFile(buff)

        for rem_output in rem_result[category]["outputs"]:
            ser = get_serializer(rem_output["media_type"])
            rem_data = ser.deserialize(zipfileobj.read(rem_output["filename"]), json_serializable)
            read[category].append(
                {
                    "title": rem_output["title"],
                    "media_type": rem_output["media_type"],
                    "data": rem_data,
                }
            )
    f = time.time()
    print(f"Read finished in {f-s}s")
    return read
