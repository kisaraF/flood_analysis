import os
import json
from Logger import init_logger as lg
from datetime import datetime
import duckdb
import polars as pl
import re


def init_db() -> None:
    conn = duckdb.connect("flood_db.duckdb")
    return conn


def get_json() -> list:
    json_ls = os.listdir(f"{os.getcwd()}/Data/JSON")
    epoch = max([i.split("_")[-1].split(".")[0] for i in json_ls])
    json_file = f"{os.getcwd()}/Data/JSON/water_level_{epoch}.json"
    with open(json_file, "r") as jf:
        json_obj = json.load(jf)
        col_objs = json_obj[:2]
        content_objs = json_obj[2:]
    lg.logging.info(f"Latest file 'water_level_{epoch}.json' was selected")
    return col_objs, content_objs, epoch


def process_headers(json_ls: list) -> list:
    header_len = len(json_ls[0].keys())
    header_columns = []
    for i in range(0, header_len):
        header_txt = json_ls[0][str(i)]
        subheader_txt = json_ls[1][str(i)]
        if header_txt != "":
            header_txt = header_txt.replace("\n", "").strip()
            header_columns.append(header_txt)
        elif header_txt == "":
            header_txt = subheader_txt
            header_txt = header_txt.replace("\n", "").strip()
            header_columns.append(header_txt)

    return header_columns


def process_content(json_ls: list) -> list:
    struct = []
    obj_len = len(json_ls[0].keys())
    for i in json_ls:
        row = [i[str(j)].replace("\n", "").strip() for j in range(0, obj_len)]
        struct.append(row)

    return struct


def process_dataframe(
    header_cols: list, content_rows: list, epoch: str
) -> pl.DataFrame:
    # River Basins replacement
    with open(f"{os.getcwd()}/maps/river_basinc.json", "r") as rbj:
        river_basin_map = json.load(rbj)
    epoch_h = datetime.fromtimestamp(int(epoch)).strftime("%Y%m%d%H%M%S")
    df = pl.DataFrame(content_rows, schema=header_cols)
    df = df.with_columns(
        pl.lit(epoch_h).alias("report_timestamp"),  # Adding a report_timestamp
        pl.lit(epoch_h[:8]).alias("report_date"),  # Adding a report date
        pl.col("Tributory/River").replace(river_basin_map).alias("River Basin"),
        pl.when(pl.col("Gauging Station") == "Yaka Wewa")
        .then(pl.lit("Mukunu Oya"))
        .otherwise(pl.col("Tributory/River").str.strip_chars())
        .alias("Tributory/River"),
        pl.col("Alert Level").cast(pl.Float64),
        pl.col("Minor Flood Level").cast(pl.Float64),
        pl.col("Major Flood Level").cast(pl.Float64),
    )
    # Transforming dynamic columns
    pattern = re.compile(
        r"^(Water Level.*|.*RF.*) at \d{1,2}([:.]\d{2})? ?[ap]m$", flags=re.IGNORECASE
    )
    dynamic_cols = [i for i in df.columns if pattern.match(i)]

    for i in dynamic_cols:
        df = df.with_columns(pl.col(i).str.replace_all(r"NA|-", "0").cast(pl.Float64))

    lg.logging.info("Polars data frame created and processed")
    return df


def insert_to_db(df: pl.DataFrame, tb_id: str) -> None:
    tb_id_conv = datetime.fromtimestamp(int(tb_id)).strftime("%Y%m%d%H%M")
    conn = init_db()
    ddl_str = f"""
CREATE OR REPLACE TABLE incidents.report_{tb_id_conv} AS
SELECT *
FROM df
"""
    conn.execute(ddl_str)
    lg.logging.info(
        "Written the data frame to the DuckDB database as an independent incident table"
    )


def main():
    log_type = "parse_json"
    lg.init_logger(log_type)

    header_cols, content_objs, epoch = get_json()
    # print(content_objs)
    header_columns = process_headers(header_cols)
    cotent_rows = process_content(content_objs)
    df = process_dataframe(header_columns, cotent_rows, epoch)
    insert_to_db(df, epoch)


if __name__ == "__main__":
    main()
