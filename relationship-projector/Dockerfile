FROM python:3.12-slim@sha256:78387bc3881b8273120a12ebe6c1ab22b018ccc2c9adf565ae1ac9b536e184ea

WORKDIR /app
COPY projector/requirements.lock ./requirements.lock
RUN pip install --no-cache-dir --require-hashes -r requirements.lock
COPY relationship-projector/main.py ./main.py
COPY relationship-projector/temporal_facts.py ./temporal_facts.py
COPY relationship-projector/test_main.py ./test_main.py

USER 65532:65532
CMD ["python", "main.py"]
