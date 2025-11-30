FROM python:3.11-slim

WORKDIR /app

ENV PYTHONUNBUFFERED=1 \
    PIP_NO_CACHE_DIR=1 \
    STREAMLIT_SERVER_PORT=8501 \
    STREAMLIT_SERVER_ADDRESS=0.0.0.0 \
    MAPBOX_API_KEY=pk.eyJ1IjoibGVvbmd3eiIsImEiOiJjbWlrcTBucTIxZ2xlM2dwZmozZjlscThxIn0.cmwwNeWoWdhCW3e1RA1zog

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY . .

EXPOSE 8501

CMD ["streamlit", "run", "src/dashboard/app.py"]
