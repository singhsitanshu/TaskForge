FROM python:3.13-slim

WORKDIR /workspace

COPY api/requirements.txt ./api/requirements.txt
COPY tests/requirements.txt ./tests/requirements.txt
RUN pip install --no-cache-dir \
    --requirement api/requirements.txt \
    --requirement tests/requirements.txt

COPY api/app ./api/app
COPY migrations ./migrations
COPY tests/test_migrations.py ./tests/test_migrations.py
COPY tests/test_task_api.py ./tests/test_task_api.py

ENV PYTHONPATH=/workspace/api

CMD ["pytest", "-q", "tests/test_migrations.py", "tests/test_task_api.py"]
