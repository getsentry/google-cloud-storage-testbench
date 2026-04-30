#!/usr/bin/env python3
#
# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Tests for GCS S3-compatible XML multipart upload API."""

import hashlib
import json
import os
import re
import unittest
import xml.etree.ElementTree as ET

from testbench import rest_server

_NS = "http://s3.amazonaws.com/doc/2006-03-01/"


def _ns(tag):
    return "{%s}%s" % (_NS, tag)


def _create_bucket(client, name="bucket-name"):
    response = client.post("/storage/v1/b", data=json.dumps({"name": name}))
    assert response.status_code == 200, response.data
    return name


def _initiate(client, bucket, key, headers=None, base_url=None):
    kwargs = {"query_string": {"uploads": ""}}
    if headers:
        kwargs["headers"] = headers
    if base_url:
        kwargs["base_url"] = base_url
    response = client.post(
        "/%s/%s" % (bucket, key) if base_url is None else "/%s" % key,
        **kwargs,
    )
    assert response.status_code == 200, response.data
    root = ET.fromstring(response.data)
    upload_id = root.findtext(_ns("UploadId"))
    assert upload_id is not None
    return upload_id, response


def _upload_part(client, bucket, key, upload_id, part_number, data, base_url=None):
    path = "/%s/%s" % (bucket, key) if base_url is None else "/%s" % key
    kwargs = {
        "query_string": {"uploadId": upload_id, "partNumber": str(part_number)},
        "data": data,
    }
    if base_url:
        kwargs["base_url"] = base_url
    response = client.put(path, **kwargs)
    assert response.status_code == 200, response.data
    etag = response.headers.get("ETag")
    assert etag is not None
    return etag


def _complete(client, bucket, key, upload_id, parts, base_url=None):
    """parts is a list of (part_number, etag) tuples."""
    xml_parts = "".join(
        "<Part><PartNumber>%d</PartNumber><ETag>%s</ETag></Part>" % (pn, etag)
        for pn, etag in parts
    )
    body = "<CompleteMultipartUpload>%s</CompleteMultipartUpload>" % xml_parts
    path = "/%s/%s" % (bucket, key) if base_url is None else "/%s" % key
    kwargs = {
        "query_string": {"uploadId": upload_id},
        "data": body.encode("utf-8"),
        "content_type": "application/xml",
    }
    if base_url:
        kwargs["base_url"] = base_url
    response = client.post(path, **kwargs)
    return response


def _compute_expected_multipart_etag(part_datas):
    binary = b""
    for data in part_datas:
        binary += hashlib.md5(data).digest()
    composite = hashlib.md5(binary).hexdigest()
    return '"%s-%d"' % (composite, len(part_datas))


class TestXmlMultipartUpload(unittest.TestCase):
    def setUp(self):
        rest_server.db.clear()
        rest_server.server.config["PREFERRED_URL_SCHEME"] = "https"
        rest_server.server.config["SERVER_NAME"] = "storage.googleapis.com"
        rest_server.root.config["PREFERRED_URL_SCHEME"] = "https"
        rest_server.root.config["SERVER_NAME"] = "storage.googleapis.com"
        self.client = rest_server.server.test_client(allow_subdomain_redirects=True)
        os.environ.pop("GOOGLE_CLOUD_CPP_STORAGE_TEST_BUCKET_NAME", None)

    # ------------------------------------------------------------------
    # 1. Path-style happy path
    # ------------------------------------------------------------------
    def test_path_style_happy_path(self):
        bucket = _create_bucket(self.client)
        part1 = b"a" * (5 * 1024 * 1024)
        part2 = b"b" * (5 * 1024 * 1024)
        part3 = b"c" * 100  # last part can be small

        upload_id, _ = _initiate(
            self.client,
            bucket,
            "obj.bin",
            headers={
                "Content-Type": "text/plain",
                "x-goog-meta-color": "blue",
            },
        )

        etag1 = _upload_part(self.client, bucket, "obj.bin", upload_id, 1, part1)
        etag2 = _upload_part(self.client, bucket, "obj.bin", upload_id, 2, part2)
        etag3 = _upload_part(self.client, bucket, "obj.bin", upload_id, 3, part3)

        response = _complete(
            self.client,
            bucket,
            "obj.bin",
            upload_id,
            [(1, etag1), (2, etag2), (3, etag3)],
        )
        self.assertEqual(response.status_code, 200, msg=response.data)

        root = ET.fromstring(response.data)
        self.assertEqual(root.findtext(_ns("Bucket")), bucket)
        self.assertEqual(root.findtext(_ns("Key")), "obj.bin")
        self.assertIsNotNone(root.findtext(_ns("ETag")))

        expected_etag = _compute_expected_multipart_etag([part1, part2, part3])
        self.assertEqual(root.findtext(_ns("ETag")), expected_etag)
        self.assertEqual(response.headers.get("ETag"), expected_etag)

        # GET the object and verify contents
        response = self.client.get("/%s/obj.bin" % bucket)
        self.assertEqual(response.status_code, 200, msg=response.data)
        self.assertEqual(response.data, part1 + part2 + part3)

        # Verify metadata round-trips via JSON API
        response = self.client.get("/storage/v1/b/%s/o/obj.bin" % bucket)
        self.assertEqual(response.status_code, 200)
        metadata = json.loads(response.data)
        self.assertEqual(metadata.get("contentType"), "text/plain")
        self.assertEqual(metadata.get("metadata", {}).get("color"), "blue")

    # ------------------------------------------------------------------
    # 2. Virtual-host (subdomain) happy path
    # ------------------------------------------------------------------
    def test_subdomain_happy_path(self):
        bucket = _create_bucket(self.client)
        base_url = "https://%s.storage.googleapis.com" % bucket
        part1 = b"x" * (5 * 1024 * 1024)
        part2 = b"y" * 42

        upload_id, _ = _initiate(self.client, bucket, "sub.txt", base_url=base_url)
        etag1 = _upload_part(
            self.client, bucket, "sub.txt", upload_id, 1, part1, base_url=base_url
        )
        etag2 = _upload_part(
            self.client, bucket, "sub.txt", upload_id, 2, part2, base_url=base_url
        )

        response = _complete(
            self.client,
            bucket,
            "sub.txt",
            upload_id,
            [(1, etag1), (2, etag2)],
            base_url=base_url,
        )
        self.assertEqual(response.status_code, 200, msg=response.data)
        root = ET.fromstring(response.data)
        self.assertEqual(
            root.findtext(_ns("Location")),
            "%s/sub.txt" % base_url,
        )

        # GET via subdomain
        response = self.client.get("/sub.txt", base_url=base_url)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, part1 + part2)

    # ------------------------------------------------------------------
    # 3. Abort
    # ------------------------------------------------------------------
    def test_abort(self):
        bucket = _create_bucket(self.client)
        upload_id, _ = _initiate(self.client, bucket, "abort.txt")
        _upload_part(self.client, bucket, "abort.txt", upload_id, 1, b"data")

        # Abort
        response = self.client.delete(
            "/%s/abort.txt" % bucket,
            query_string={"uploadId": upload_id},
        )
        self.assertEqual(response.status_code, 204)

        # Upload part after abort should 404
        response = self.client.put(
            "/%s/abort.txt" % bucket,
            query_string={"uploadId": upload_id, "partNumber": "2"},
            data=b"more",
        )
        self.assertEqual(response.status_code, 404)

        # Object should not exist
        response = self.client.get("/%s/abort.txt" % bucket)
        self.assertEqual(response.status_code, 404)

    def test_abort_rejects_non_xml_upload_id(self):
        bucket = _create_bucket(self.client)
        response = self.client.post(
            "/upload/storage/v1/b/%s/o" % bucket,
            query_string={"uploadType": "resumable", "name": "abort.txt"},
            content_type="application/json",
            data=json.dumps({"name": "abort.txt"}),
        )
        self.assertEqual(response.status_code, 200, msg=response.data)

        location = response.headers.get("Location")
        self.assertIsNotNone(location)
        match = re.search("[&?]upload_id=([^&]+)", location)
        self.assertIsNotNone(match, msg=location)
        upload_id = match.group(1)

        response = self.client.delete(
            "/%s/abort.txt" % bucket,
            query_string={"uploadId": upload_id},
        )
        self.assertEqual(response.status_code, 404, msg=response.data)

    def test_upload_part_rejects_invalid_part_numbers(self):
        bucket = _create_bucket(self.client)
        upload_id, _ = _initiate(self.client, bucket, "invalid-part.bin")

        for part_number in ("0", "-1", "10001", "abc"):
            response = self.client.put(
                "/%s/invalid-part.bin" % bucket,
                query_string={"uploadId": upload_id, "partNumber": part_number},
                data=b"data",
            )
            self.assertEqual(response.status_code, 400, msg=response.data)
            self.assertIn(b"partNumber is invalid", response.data)

        response = self.client.get(
            "/%s/invalid-part.bin" % bucket,
            query_string={"uploadId": upload_id},
        )
        self.assertEqual(response.status_code, 200, msg=response.data)
        root = ET.fromstring(response.data)
        self.assertEqual(root.findall(_ns("Part")), [])

    def test_put_rejects_partial_multipart_query_params(self):
        bucket = _create_bucket(self.client)
        upload_id, _ = _initiate(self.client, bucket, "partial-query.bin")

        for query_string in (
            {"uploadId": upload_id},
            {"partNumber": "1"},
        ):
            response = self.client.put(
                "/%s/partial-query.bin" % bucket,
                query_string=query_string,
                data=b"unexpected-object-write",
            )
            self.assertEqual(response.status_code, 400, msg=response.data)
            self.assertIn(
                b"multipart upload query parameters is invalid", response.data
            )

        response = self.client.get("/%s/partial-query.bin" % bucket)
        self.assertEqual(response.status_code, 404, msg=response.data)

    def test_subdomain_put_rejects_partial_multipart_query_params(self):
        bucket = _create_bucket(self.client)
        base_url = "https://%s.storage.googleapis.com" % bucket
        upload_id, _ = _initiate(
            self.client, bucket, "partial-query.bin", base_url=base_url
        )

        for query_string in (
            {"uploadId": upload_id},
            {"partNumber": "1"},
        ):
            response = self.client.put(
                "/partial-query.bin",
                base_url=base_url,
                query_string=query_string,
                data=b"unexpected-object-write",
            )
            self.assertEqual(response.status_code, 400, msg=response.data)
            self.assertIn(
                b"multipart upload query parameters is invalid", response.data
            )

        response = self.client.get("/partial-query.bin", base_url=base_url)
        self.assertEqual(response.status_code, 404, msg=response.data)

    # ------------------------------------------------------------------
    # 4. ListParts
    # ------------------------------------------------------------------
    def test_list_parts(self):
        bucket = _create_bucket(self.client)
        upload_id, _ = _initiate(self.client, bucket, "lp.bin")
        data1 = b"a" * 100
        data2 = b"b" * 200
        data3 = b"c" * 300
        etag1 = _upload_part(self.client, bucket, "lp.bin", upload_id, 1, data1)
        etag2 = _upload_part(self.client, bucket, "lp.bin", upload_id, 2, data2)
        etag3 = _upload_part(self.client, bucket, "lp.bin", upload_id, 3, data3)

        # List all
        response = self.client.get(
            "/%s/lp.bin" % bucket,
            query_string={"uploadId": upload_id},
        )
        self.assertEqual(response.status_code, 200)
        root = ET.fromstring(response.data)
        parts = root.findall(_ns("Part"))
        self.assertEqual(len(parts), 3)
        self.assertEqual(parts[0].findtext(_ns("PartNumber")), "1")
        self.assertEqual(parts[0].findtext(_ns("ETag")), etag1)
        self.assertEqual(parts[0].findtext(_ns("Size")), "100")
        self.assertEqual(parts[1].findtext(_ns("Size")), "200")
        self.assertEqual(parts[2].findtext(_ns("Size")), "300")
        self.assertEqual(root.findtext(_ns("IsTruncated")), "false")

    def test_list_parts_pagination(self):
        bucket = _create_bucket(self.client)
        upload_id, _ = _initiate(self.client, bucket, "lpp.bin")
        _upload_part(self.client, bucket, "lpp.bin", upload_id, 1, b"a")
        _upload_part(self.client, bucket, "lpp.bin", upload_id, 2, b"b")
        _upload_part(self.client, bucket, "lpp.bin", upload_id, 3, b"c")

        # Page 1: max-parts=2
        response = self.client.get(
            "/%s/lpp.bin" % bucket,
            query_string={"uploadId": upload_id, "max-parts": "2"},
        )
        self.assertEqual(response.status_code, 200)
        root = ET.fromstring(response.data)
        parts = root.findall(_ns("Part"))
        self.assertEqual(len(parts), 2)
        self.assertEqual(root.findtext(_ns("IsTruncated")), "true")
        self.assertEqual(root.findtext(_ns("NextPartNumberMarker")), "2")

        # Page 2: part-number-marker=2
        response = self.client.get(
            "/%s/lpp.bin" % bucket,
            query_string={
                "uploadId": upload_id,
                "max-parts": "2",
                "part-number-marker": "2",
            },
        )
        self.assertEqual(response.status_code, 200)
        root = ET.fromstring(response.data)
        parts = root.findall(_ns("Part"))
        self.assertEqual(len(parts), 1)
        self.assertEqual(parts[0].findtext(_ns("PartNumber")), "3")
        self.assertEqual(root.findtext(_ns("IsTruncated")), "false")

    # ------------------------------------------------------------------
    # 5. Min-part-size enforcement
    # ------------------------------------------------------------------
    def test_min_part_size_rejected(self):
        bucket = _create_bucket(self.client)
        upload_id, _ = _initiate(self.client, bucket, "small.bin")
        etag1 = _upload_part(self.client, bucket, "small.bin", upload_id, 1, b"x")
        etag2 = _upload_part(self.client, bucket, "small.bin", upload_id, 2, b"y")

        response = _complete(
            self.client, bucket, "small.bin", upload_id, [(1, etag1), (2, etag2)]
        )
        self.assertEqual(response.status_code, 400, msg=response.data)
        self.assertIn(b"EntityTooSmall", response.data)

    def test_min_part_size_last_part_exempt(self):
        bucket = _create_bucket(self.client)
        upload_id, _ = _initiate(self.client, bucket, "ok.bin")
        big = b"x" * (5 * 1024 * 1024)
        small = b"y"
        etag1 = _upload_part(self.client, bucket, "ok.bin", upload_id, 1, big)
        etag2 = _upload_part(self.client, bucket, "ok.bin", upload_id, 2, small)

        response = _complete(
            self.client, bucket, "ok.bin", upload_id, [(1, etag1), (2, etag2)]
        )
        self.assertEqual(response.status_code, 200, msg=response.data)

        response = self.client.get("/%s/ok.bin" % bucket)
        self.assertEqual(response.status_code, 200)
        self.assertEqual(response.data, big + small)

    def test_complete_rejects_mismatched_route(self):
        bucket = _create_bucket(self.client)
        upload_id, _ = _initiate(self.client, bucket, "source.bin")
        etag = _upload_part(
            self.client, bucket, "source.bin", upload_id, 1, b"x" * (5 * 1024 * 1024)
        )

        response = _complete(self.client, bucket, "other.bin", upload_id, [(1, etag)])
        self.assertEqual(response.status_code, 404, msg=response.data)

        response = self.client.get("/%s/source.bin" % bucket)
        self.assertEqual(response.status_code, 404, msg=response.data)

    def test_complete_preserves_xml_preconditions(self):
        bucket = _create_bucket(self.client)
        first = self.client.put("/%s/preconditioned.bin" % bucket, data=b"original")
        self.assertEqual(first.status_code, 200, msg=first.data)

        upload_id, _ = _initiate(
            self.client,
            bucket,
            "preconditioned.bin",
            headers={"x-goog-if-generation-match": "0"},
        )
        etag = _upload_part(
            self.client,
            bucket,
            "preconditioned.bin",
            upload_id,
            1,
            b"x" * (5 * 1024 * 1024),
        )

        response = _complete(
            self.client, bucket, "preconditioned.bin", upload_id, [(1, etag)]
        )
        self.assertEqual(response.status_code, 412, msg=response.data)

        response = self.client.get("/%s/preconditioned.bin" % bucket)
        self.assertEqual(response.status_code, 200, msg=response.data)
        self.assertEqual(response.data, b"original")

    def test_complete_rejects_stale_part_etag(self):
        bucket = _create_bucket(self.client)
        upload_id, _ = _initiate(self.client, bucket, "etag-mismatch.bin")
        _upload_part(
            self.client,
            bucket,
            "etag-mismatch.bin",
            upload_id,
            1,
            b"x" * (5 * 1024 * 1024),
        )

        response = _complete(
            self.client,
            bucket,
            "etag-mismatch.bin",
            upload_id,
            [(1, '"00000000000000000000000000000000"')],
        )
        self.assertEqual(response.status_code, 400, msg=response.data)
        self.assertIn(b"InvalidPart", response.data)

        response = self.client.get("/%s/etag-mismatch.bin" % bucket)
        self.assertEqual(response.status_code, 404, msg=response.data)

    def test_complete_rejects_out_of_order_parts(self):
        bucket = _create_bucket(self.client)
        upload_id, _ = _initiate(self.client, bucket, "out-of-order.bin")
        part1 = b"x" * (5 * 1024 * 1024)
        part2 = b"y"
        etag1 = _upload_part(
            self.client, bucket, "out-of-order.bin", upload_id, 1, part1
        )
        etag2 = _upload_part(
            self.client, bucket, "out-of-order.bin", upload_id, 2, part2
        )

        response = _complete(
            self.client,
            bucket,
            "out-of-order.bin",
            upload_id,
            [(2, etag2), (1, etag1)],
        )
        self.assertEqual(response.status_code, 400, msg=response.data)
        self.assertIn(b"InvalidPartOrder", response.data)

        response = self.client.get("/%s/out-of-order.bin" % bucket)
        self.assertEqual(response.status_code, 404, msg=response.data)

    def test_complete_rejects_empty_manifest(self):
        bucket = _create_bucket(self.client)
        upload_id, _ = _initiate(self.client, bucket, "empty-manifest.bin")

        response = _complete(self.client, bucket, "empty-manifest.bin", upload_id, [])
        self.assertEqual(response.status_code, 400, msg=response.data)
        self.assertIn(b"InvalidRequest", response.data)

        response = self.client.get("/%s/empty-manifest.bin" % bucket)
        self.assertEqual(response.status_code, 404, msg=response.data)

    def test_complete_rejects_malformed_manifest_xml(self):
        bucket = _create_bucket(self.client)
        upload_id, _ = _initiate(self.client, bucket, "malformed.xml")

        response = self.client.post(
            "/%s/malformed.xml" % bucket,
            query_string={"uploadId": upload_id},
            data=b"<CompleteMultipartUpload><Part>",
            content_type="application/xml",
        )
        self.assertEqual(response.status_code, 400, msg=response.data)
        self.assertIn(b"CompleteMultipartUpload XML is invalid", response.data)

    def test_complete_rejects_non_numeric_part_number(self):
        bucket = _create_bucket(self.client)
        upload_id, _ = _initiate(self.client, bucket, "bad-part-number.xml")

        response = self.client.post(
            "/%s/bad-part-number.xml" % bucket,
            query_string={"uploadId": upload_id},
            data=(
                b"<CompleteMultipartUpload>"
                b"<Part><PartNumber>abc</PartNumber><ETag>etag</ETag></Part>"
                b"</CompleteMultipartUpload>"
            ),
            content_type="application/xml",
        )
        self.assertEqual(response.status_code, 400, msg=response.data)
        self.assertIn(b"CompleteMultipartUpload XML is invalid", response.data)

    # ------------------------------------------------------------------
    # 6. Initiate response shape
    # ------------------------------------------------------------------
    def test_initiate_response_xml_shape(self):
        bucket = _create_bucket(self.client)
        _, response = _initiate(self.client, bucket, "shape.txt")
        root = ET.fromstring(response.data)
        self.assertTrue(
            root.tag.endswith("InitiateMultipartUploadResult"),
            msg="unexpected root tag: %s" % root.tag,
        )
        self.assertEqual(root.findtext(_ns("Bucket")), bucket)
        self.assertEqual(root.findtext(_ns("Key")), "shape.txt")
        self.assertIsNotNone(root.findtext(_ns("UploadId")))
        self.assertEqual(root.tag, _ns("InitiateMultipartUploadResult"))

    # ------------------------------------------------------------------
    # 7. ETag math sanity
    # ------------------------------------------------------------------
    def test_etag_math(self):
        bucket = _create_bucket(self.client)
        parts_data = [b"alpha", b"beta", b"gamma"]

        upload_id, _ = _initiate(self.client, bucket, "etag.bin")
        etags = []
        for i, data in enumerate(parts_data, 1):
            etag = _upload_part(self.client, bucket, "etag.bin", upload_id, i, data)
            expected = '"%s"' % hashlib.md5(data).hexdigest()
            self.assertEqual(etag, expected)
            etags.append(etag)

        # For this test, skip min-part-size by using a single part
        # (all non-final parts would fail the 5MiB check).
        # Instead, just verify the part ETags are correct (above).
        # Use a separate single-part upload to verify the complete ETag.
        upload_id2, _ = _initiate(self.client, bucket, "etag2.bin")
        single_data = b"hello world"
        single_etag = _upload_part(
            self.client, bucket, "etag2.bin", upload_id2, 1, single_data
        )
        response = _complete(
            self.client, bucket, "etag2.bin", upload_id2, [(1, single_etag)]
        )
        self.assertEqual(response.status_code, 200)
        expected = _compute_expected_multipart_etag([single_data])
        root = ET.fromstring(response.data)
        self.assertEqual(root.findtext(_ns("ETag")), expected)

    # ------------------------------------------------------------------
    # 8. No regression: POST without ?uploads or ?uploadId still → 501
    # ------------------------------------------------------------------
    def test_post_without_multipart_params_returns_501(self):
        response = self.client.post("/bucket-name/object-name")
        self.assertEqual(response.status_code, 501)


if __name__ == "__main__":
    unittest.main()
