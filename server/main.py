from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from server.database import engine
import orjson
import pyarrow as pa
import io

app = FastAPI()

from typing import Literal, Optional

class QueryRequest(BaseModel):
    query: str
    format: Literal['ndjson', 'arrow'] = 'ndjson'
    limit: Optional[int] = None

@app.post("/stream")
async def stream_endpoint(request: QueryRequest):
    """
    Executes a SQL query and streams the results in the requested format.
    Supports optional limit to restrict result size.
    """
    
    # Construct the final query with limit if provided
    final_query = request.query
    params = {}
    
    # We wrap the query to apply limit safely
    if request.limit is not None:
        final_query = f"SELECT * FROM ({request.query}) AS subq LIMIT :limit"
        params['limit'] = request.limit

    async def generate_ndjson(conn):
        # We pass params to execute/stream
        result = await conn.stream(
            text(final_query),
            params,
            execution_options={"yield_per": 1000}
        )
        async for row in result:
            data = dict(row._mapping)
            yield orjson.dumps(data).decode('utf-8') + "\n"

    async def generate_arrow(conn):
        result = await conn.stream(
            text(final_query),
            params,
            execution_options={"yield_per": 1000}
        )
        
        batch_size = 1000
        current_batch = []
        schema = None
        writer = None
        sink = io.BytesIO()
        
        async for row in result:
            current_batch.append(dict(row._mapping))
            
            if len(current_batch) >= batch_size:
                if schema is None:
                    table = pa.Table.from_pylist(current_batch)
                    schema = table.schema
                    writer = pa.RecordBatchStreamWriter(sink, schema)
                    writer.write_table(table)
                else:
                    table = pa.Table.from_pylist(current_batch, schema=schema)
                    writer.write_table(table)
                    
                yield sink.getvalue()
                sink.truncate(0)
                sink.seek(0)
                current_batch = []
        
        if current_batch:
            if schema is None:
                table = pa.Table.from_pylist(current_batch)
                schema = table.schema
                writer = pa.RecordBatchStreamWriter(sink, schema)
                writer.write_table(table)
            else:
                table = pa.Table.from_pylist(current_batch, schema=schema)
                writer.write_table(table)
            
            yield sink.getvalue()
            sink.truncate(0)
            sink.seek(0)
        
        if writer:
            writer.close()
            yield sink.getvalue()

    async def generate():
        async with engine.connect() as conn:
            if request.format == 'ndjson':
                async for chunk in generate_ndjson(conn):
                    yield chunk
            elif request.format == 'arrow':
                async for chunk in generate_arrow(conn):
                    yield chunk

    media_type = "application/x-ndjson" if request.format == 'ndjson' else "application/vnd.apache.arrow.stream"
    return StreamingResponse(generate(), media_type=media_type)

