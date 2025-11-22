# Async HTTP Streaming Example

This project demonstrates how to stream data from a PostgreSQL database via FastAPI using NDJSON and Apache Arrow formats.

## Prerequisites

- Docker and Docker Compose
- Python 3.9+

## Setup

1.  **Start the Database:**
    ```bash
    docker-compose up -d
    ```

2.  **Create a Virtual Environment and Install Dependencies:**
    ```bash
    python3 -m venv venv
    source venv/bin/activate
    pip install -r requirements.txt
    ```

3.  **Start the Server:**
    ```bash
    uvicorn server.main:app --reload --port 8000
    ```

4.  **Run the Client:**
    ```bash
    python client/client.py
    ```

## Features

- **Database**: PostgreSQL 16 running in Docker.
- **Backend**: FastAPI with `asyncpg` and `SQLAlchemy` (async).
- **Streaming**:
    - **NDJSON**: Newline Delimited JSON.
    - **Arrow**: Apache Arrow IPC Stream.
- **Efficiency**: Uses `yield_per(1000)` to fetch data in chunks from the database and stream it immediately to the client, keeping memory usage low.

## Notes

- **Security Warning**: The API accepts raw SQL queries for demonstration purposes. **DO NOT USE THIS IN PRODUCTION** as it is vulnerable to SQL Injection.
- The client demonstrates how to consume the Arrow stream using a background thread to bridge the async network download with the blocking PyArrow reader.
