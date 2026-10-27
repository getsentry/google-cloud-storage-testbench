# Copyright 2021 Google LLC
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
FROM python:3.12@sha256:9e615ab2a4e8c67f9638fa93002ac364647c2c2c1b66925ef3f8832fda354cd9

EXPOSE 9000
WORKDIR /opt/storage-testbench

# The base image's ca-certificates and libbrotli predate the eMudhra root
# removal and the CVE-2026-111497 fix.
RUN apt-get update \
    && apt-get install -y --only-upgrade ca-certificates libbrotli1 libbrotli-dev \
    && rm -rf /var/lib/apt/lists/*

COPY . /opt/storage-testbench/

RUN python3 -m pip install -e .

CMD ["python3", \
      "testbench_run.py", \
      "0.0.0.0", \
      "9000", \
      "10"]
