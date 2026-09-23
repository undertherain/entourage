# Generic Entourage worker runtime. The agent folder is mounted at /agent and the
# store directory at /state; the runner passes the remaining arguments.
#
#   podman build -t localhost/entourage-runtime .
#
# Agents needing extra dependencies build FROM this image and install them.
FROM python:3.12-slim
WORKDIR /opt/entourage
COPY pyproject.toml README.md ./
COPY entourage ./entourage
RUN pip install --no-cache-dir . && rm -rf /root/.cache
RUN useradd --create-home --uid 1000 worker
USER worker
VOLUME ["/state", "/agent"]
ENTRYPOINT ["python", "-m", "entourage.worker"]
