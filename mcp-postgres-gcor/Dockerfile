FROM python:3.12-slim

WORKDIR /app
COPY requirements.lock .
RUN pip install --no-cache-dir --require-hashes -r requirements.lock
COPY server.py .

ENV FASTMCP_HOST=0.0.0.0
ENV FASTMCP_PORT=8765
EXPOSE 8765
HEALTHCHECK --interval=15s --timeout=5s --start-period=10s --retries=3 \
    CMD python -c "import os,socket; socket.create_connection(('127.0.0.1',int(os.getenv('FASTMCP_PORT','8765'))),timeout=3).close()"
# Run unprivileged even outside Compose; the image needs no writable path but /tmp.
RUN useradd --system --uid 10001 --no-create-home gcor
USER 10001:10001
CMD ["python", "server.py"]
