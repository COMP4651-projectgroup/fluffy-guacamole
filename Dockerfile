FROM python:3.11-slim-bookworm

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    MAPBOX_API_KEY=pk.eyJ1IjoibGVvbmd3eiIsImEiOiJjbWlrcTBucTIxZ2xlM2dwZmozZjlscThxIn0.cmwwNeWoWdhCW3e1RA1zog

COPY requirements.txt .
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jdk-headless \
    && rm -rf /var/lib/apt/lists/* \
    && pip install --no-cache-dir -r requirements.txt

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64
ENV PATH="${JAVA_HOME}/bin:${PATH}"

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "src/dashboard/app.py"]
