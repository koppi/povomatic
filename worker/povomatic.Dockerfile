# povomatic - distributed POV-Ray rendering on Kubernetes
# Copyright (C) 2026 Jakob Flierl
#
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU Affero General Public License as published
# by the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU Affero General Public License for more details.
#
# You should have received a copy of the GNU Affero General Public License
# along with this program.  If not, see <https://www.gnu.org/licenses/>.

# Debian sid, because povray 3.8 needs it. The 3.8 beta is packaged only in
# Debian experimental and depends on glibc 2.43, where the trixie based
# python:3.11-slim this used to build on carries 2.41, so the beta cannot be
# grafted onto a stable base: the base itself has to move. Only povray comes
# from experimental; everything else is ordinary sid.
FROM debian:sid-slim

RUN apt-get update && apt-get install -y --no-install-recommends ca-certificates \
 && echo "deb http://deb.debian.org/debian experimental main" > /etc/apt/sources.list.d/experimental.list \
 && apt-get update \
 && apt-get install -y --no-install-recommends -t experimental \
      povray \
 && apt-get install -y --no-install-recommends \
      ffmpeg \
      mesa-va-drivers \
      postgresql-client \
      python3 \
      python3-venv \
 && rm -rf /var/lib/apt/lists/*

# A virtualenv rather than pip into the system python: sid marks its python
# externally managed, and pip refuses to touch it.
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app

RUN pip install --no-cache-dir psycopg2-binary flask flask-socketio gevent gevent-websocket waitress

COPY worker/worker.py .
COPY worker/api.py .
COPY dashboard/app.py dashboard/
COPY dashboard/templates/ dashboard/templates/

# Run worker
CMD ["python", "worker.py"]
