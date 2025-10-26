FROM postgis/postgis:18-3.6

RUN apt-get update \
 && apt-get install -y --no-install-recommends \
      build-essential git postgresql-server-dev-18 \
 && rm -rf /var/lib/apt/lists/*

RUN git clone https://github.com/pgvector/pgvector /tmp/pgvector \
 && make -C /tmp/pgvector \
 && make -C /tmp/pgvector install \
 && rm -rf /tmp/pgvector