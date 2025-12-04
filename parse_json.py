import os
import json
from Logger import init_logger as lg
from datetime import datetime
import duckdb
import polars as pl
import re


def init_db() -> None:
    conn = duckdb.connect("flood_db.duckdb")
    create_schema = """
CREATE SCHEMA if not exists flood_db.incidents
"""
    conn.execute(create_schema)
    return conn


def get_json() -> list:
    json_ls = os.listdir(f"{os.getcwd()}/Data/JSON")
    epoch = max(
        [i.split("_")[-1].split(".")[0] for i in json_ls]
    )  # Looking for the latest json by getting the max epoch report time
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
    with open(
        f"{os.getcwd()}/maps/river_basinc.json", "r"
    ) as rbj:  # River basins are not properly filled out. Mapping and filling them
        river_basin_map = json.load(rbj)
    epoch_h = datetime.fromtimestamp(int(epoch)).strftime(
        "%Y%m%d%H%M%S"
    )  # Adding a human readable timestamp for the report epoch
    df = pl.DataFrame(content_rows, schema=header_cols)
    df = df.with_columns(
        pl.lit(epoch_h).alias("report_timestamp"),  # Adding a report_timestamp
        pl.lit(epoch_h[:8]).alias("report_date"),  # Adding a report date
        pl.col("Tributory/River")
        .replace(river_basin_map)
        .alias("River Basin"),  # Properly fillinf river basins
        pl.when(pl.col("Gauging Station") == "Yaka Wewa")
        .then(pl.lit("Mukunu Oya"))
        .otherwise(pl.col("Tributory/River").str.strip_chars())
        .alias("Tributory/River"),
        pl.col("Alert Level").cast(pl.Float64),
        pl.col("Minor Flood Level").cast(pl.Float64),
        pl.col("Major Flood Level").cast(pl.Float64),
    )

    pattern = re.compile(
        r"""^                      # start of string
    (?:                         # non-capturing group for the prefix
        \d+\s*Hr\s*RF\s*in\s*mm|  # matches '15 Hr RF in mm' etc.
        Water\s+Level           # matches 'Water Level'
    )
    .*                          # any text in between
    at\s+                       # 'at ' literal
    \d{1,2}([:.]\d{2})?         # hour with optional minutes
    \s*                          # optional space
    (?:am|pm|noon|midnight)     # time suffix
    $                            # end of string
    """,
        flags=re.IGNORECASE | re.VERBOSE,
    )

    dynamic_cols = [i for i in df.columns if pattern.match(i)]
    # Individually identifying each dynamic column for later transformations
    min_water_level_col_id = min(
        [
            int(i.split("at ")[-1].split(":")[0])
            for i in dynamic_cols
            if "Water Level" in i
        ]
    )
    max_water_level_col_id = max(
        [
            int(i.split("at ")[-1].split(":")[0])
            for i in dynamic_cols
            if "Water Level" in i
        ]
    )
    max_water_level_col = [
        i
        for i in dynamic_cols
        if "Water Level" in i and str(max_water_level_col_id) in i
    ][0]
    min_water_level_col = [
        i
        for i in dynamic_cols
        if "Water Level" in i and str(min_water_level_col_id) in i
    ][0]
    rf_col = [i for i in dynamic_cols if "Water" not in i][0]
    rf_time_period = rf_col.split(" Hr")[0]

    for i in dynamic_cols:
        df = df.with_columns(
            pl.col(i).str.replace_all(r"NA|-|N.A.", "0").cast(pl.Float64)
        )  # Replace dynamic columns NA values as 0 for convenient type conversions

    df = df.with_columns(  # Derived column with analytics value
        pl.col(max_water_level_col).alias("last_hour_reported_water_level"),
        (pl.col(max_water_level_col) - pl.col(min_water_level_col)).alias(
            "last_hour_water_level_difference"
        ),
    )

    df = df.with_columns(  # breakuing into two ops since a LCA similar issue is faced
        pl.when(pl.col("last_hour_water_level_difference") > 0)
        .then(pl.lit("rising"))
        .when(pl.col("last_hour_water_level_difference") < 0)
        .then(pl.lit("falling"))
        .when(pl.col("last_hour_water_level_difference") == 0)
        .then(pl.lit("no change"))
        .otherwise(pl.lit("<!Error>"))
        .alias("water_level_change_tag"),
        pl.col(rf_col).alias("rainfall_mm"),
        pl.lit(rf_time_period).alias("rainfall_hour_interval"),
    )

    lg.logging.info("Polars data frame created and processed")
    return df


def finalize_df(df: pl.DataFrame) -> pl.DataFrame:
    rename_dict = {  # Rename columns for easier querying purposes
        "River Basin": "river_basin",
        "Tributory/River": "river",
        "Gauging Station": "gauging_station",
        "Unit": "unit",
        "Alert Level": "alert_level",
        "Minor Flood Level": "minor_flood_level",
        "Major Flood Level": "major_flood_level",
        "Remarks": "remarks",
    }
    df = df.rename(rename_dict)

    # Selecting only the needed columns
    df = df.select(
        [
            "report_timestamp",
            "report_date",
            "river_basin",
            "river",
            "gauging_station",
            "unit",
            "alert_level",
            "minor_flood_level",
            "major_flood_level",
            "last_hour_reported_water_level",
            "last_hour_water_level_difference",
            "water_level_change_tag",
            "rainfall_mm",
            "rainfall_hour_interval",
            "remarks",
        ]
    )
    lg.logging.info("Renamed columns and selected onyl the columns needed")
    return df


def insert_to_db(df: pl.DataFrame, tb_id: str) -> None:
    tb_name = "incidents_report"
    conn = init_db()

    check_tb = f"""
SELECT COUNT(*)
FROM duckdb_tables 
WHERE table_name = '{tb_name}'
"""
    exists = conn.execute(check_tb).fetchone()[0]
    if exists == 0:
        ddl_str = f"""
CREATE OR REPLACE TABLE incidents.{tb_name} AS
SELECT *
FROM df
"""
        conn.execute(ddl_str)
        lg.logging.info("Created the table and inserted first records")

    elif exists > 0:
        insert_str = f"""
INSERT INTO incidents.{tb_name}
SELECT *
FROM df
"""
        conn.execute(insert_str)

    lg.logging.info("Inserted new rows to the table")


def main():
    log_type = "parse_json"
    lg.init_logger(log_type)

    header_cols, content_objs, epoch = get_json()
    header_columns = process_headers(header_cols)
    cotent_rows = process_content(content_objs)
    df = process_dataframe(header_columns, cotent_rows, epoch)
    final_df = finalize_df(df)
    insert_to_db(final_df, epoch)


if __name__ == "__main__":
    main()
