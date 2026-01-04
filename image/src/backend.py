from fastapi import FastAPI, Query, Path, Depends
from pydantic import BaseModel
from datetime import datetime
import duckdb
from typing import Annotated

app = FastAPI()


def duckdb_conn() -> duckdb.DuckDBPyConnection:
    conn = duckdb.connect("flood_db.duckdb")
    try:
        yield conn
    finally:
        conn.close()


@app.get("/water-level")
async def get_all_incidents(
    conn: Annotated[duckdb.DuckDBPyConnection, Depends(duckdb_conn)],
    date: str | None = Query(default=None),
    date_range: list[str] | None = Query(default=None),
    river: list[str] | None = Query(default=None),
    basin: list[str] | None = Query(default=None),
    station: list[str] | None = Query(default=None),
):
    query = """
        SELECT *
        FROM flood_db.incidents.incidents_report
        WHERE 1=1
    """
    params = []

    if date is not None:
        query += " AND report_date = ?"
        params.append(date)

    if date_range is not None:
        start_date, end_date = date_range
        query += " AND report_date BETWEEN ? AND ?"
        params.extend([start_date, end_date])

    if river is not None:
        placeholders = ", ".join(["?"] * len(river))
        query += f" AND lower(river) IN ({placeholders})"
        params.extend([r.lower() for r in river])

    if basin is not None:
        placeholders = ", ".join(["?"] * len(basin))
        query += f" AND lower(river_basin) IN ({placeholders})"
        params.extend([b.lower() for b in basin])

    if station is not None:
        placeholders = ", ".join(["?"] * len(station))
        query += " AND lower(gauging_station) IN ({placeholders})"
        params.extend([s.lower() for s in station])

    return conn.execute(query, params).fetchall()
