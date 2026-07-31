# syntax=docker/dockerfile:1

ARG PYTHON_VERSION=3.12.6

FROM python:${PYTHON_VERSION}-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_ROOT_USER_ACTION=ignore

WORKDIR /app

RUN groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --create-home app \
    && chown app:app /app


FROM base AS production-dependencies

COPY requirements.lock.txt /tmp/requirements.lock.txt

RUN python -m pip install \
    --require-hashes \
    -r /tmp/requirements.lock.txt


FROM production-dependencies AS runtime

COPY --chown=app:app . /app

USER app

ENTRYPOINT ["python", "scripts/pipeline.py"]
CMD ["--help"]


FROM base AS test-dependencies

COPY requirements-dev.lock.txt /tmp/requirements-dev.lock.txt

RUN python -m pip install \
    --require-hashes \
    -r /tmp/requirements-dev.lock.txt


FROM test-dependencies AS test

COPY --chown=app:app . /app

USER app

CMD ["python", "-m", "pytest", "tests", "-q"]
