FROM python:3.13-slim

WORKDIR /workspace

COPY tests/requirements.txt ./tests/requirements.txt
RUN pip install --no-cache-dir --requirement tests/requirements.txt

COPY migrations ./migrations
COPY tests/test_migrations.py ./tests/test_migrations.py

CMD ["pytest", "-q", "tests/test_migrations.py"]

