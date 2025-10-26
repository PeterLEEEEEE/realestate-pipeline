# syntax=docker/dockerfile:1.7-labs
FROM astrocrpublic.azurecr.io/runtime:3.1-2-python-3.12

USER root

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl gcc g++ \
    && rm -rf /var/lib/apt/lists/*

# uv 설치 (이미 있으면 재설치만 되고, 심볼릭 링크는 있을 때 건너뜀)
ENV PATH="/root/.local/bin:${PATH}"
RUN curl -LsSf https://astral.sh/uv/install.sh | sh \
 && { [ -e /usr/local/bin/uv ] || ln -s /root/.local/bin/uv /usr/local/bin/uv; } \
 && { [ -e /usr/local/bin/uvx ] || ln -s /root/.local/bin/uvx /usr/local/bin/uvx; }

USER astro
WORKDIR /usr/local/airflow

# requirements 먼저 복사해서 레이어 캐시 최대화
COPY requirements.txt ./
RUN --mount=type=cache,target=/home/astro/.cache/uv \
    uv pip install --system -r requirements.txt
