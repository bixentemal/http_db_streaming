from fastapi import FastAPI, HTTPException
from pydantic import BaseModel
from fastapi.responses import StreamingResponse
from sqlalchemy import text
from server.database import engine
import orjson
import pyarrow as pa
import io

app = FastAPI()

from typing import Literal

class QueryRequest(BaseModel):
    query: str
    format: Literal['ndjson', 'arrow'] = 'ndjson'

@app.post("/stream")
async def stream_endpoint(request: QueryRequest):
    """
    Executes a SQL query and streams the results in the requested format.
    """
    async def generate_ndjson(conn):
        result = await conn.stream(
            text(request.query),
            execution_options={"yield_per": 1000}
        )
        async for row in result:
            data = dict(row._mapping)
            yield orjson.dumps(data).decode('utf-8') + "\n"

    async def generate_arrow(conn):
        result = await conn.stream(
            text(request.query),
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

