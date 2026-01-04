# Flood Data Collection

This project aims to collect data from Sri Lanka's Disaster Management Center's official website's river bain water level reports. The project is inspired by Nuwan Senarathne's original work. While Nuwan Senarathne's work is tremendous, I felt the need for providing a clean dataset for analysts, climate experts through a form of CSV as the original data is in PDF reports which could be tedious and daunting to extract data from if chose to do it manually.

## Data Source

* URL - [DMC](https://www.dmc.gov.lk/index.php?option=com_dmcreports&view=reports&Itemid=277&report_type_id=6&lang=en&limitstart=0)
* Format - PDF

## Extracting Data

As this was a rapid prototype built fast, the initial version requires to source data from the DMC's website. In other words, manual download of reports are required. Refer to the future works section below to learn what's awaiting next for this project.

### Extracting data from the PDF reports

1. Move the downloaded PDF reports to `Data/PDF` folder.
2. In the project root directory, run command `python3 bulk_process.py` using your terminal of choice. (`bulk_process.py` script utilizes "extract_pdf.py", "parse_json.py" scripts to ingest data to the duckdb database)

### About ingestion process

- `extract_pdf.py` will scan for all the PDFs in the "Data/PDF" folder and extract their table's (within the PDF) and convert it to a JSON file and store in "Data/JSON" folder. For this, `camelot` library is used which is an excellent library to extract PDF tables.
- Next, `parse_json.py` will extract the contents, clean them, and insert into the duckdb database.

__Derived Attributes__

The original PDF report consists of dynamically changing columns such as,
- "Water Level at xxxx"
- "X HR RF in mm at xxxx"

Therefore, the following derived columns are introduced:
1. last hour reported water level
2. lsat hour water level difference
3. rainfall in mm
4. rainfall hour interval

Through these derived attributes, we can make all the incident report data,
- __Normalized__ with one uniform measure
- __Centralized__ for better analytical reporting (for example, we can derive metrics like "average hourly rainfall in mm", etc.)

## Querying the Data

If you are familiar with SQL, all you have to do is, use a database manager like ___[DBeaver](https://dbeaver.io/download/)___

### Sample Queries

1. Get incidents for a specific date

```sql
SELECT *
FROM flood_db.incidents.incidents_report
WHERE report_date = "20251130"
```

2. Get incidents for a date range

```sql
SELECT *
FROM flood_db.incidents.incidents_report
WHERE report_date BETWEEN "20251130" AND "20251206"
```

3. Get incidents of a given gauging station on a given date

```sql
SELECT *
FROM flood_db.incidents.incidents_report
WHERE report_date = "20251130"
AND gauging_station = "Nagalagam Street"
```

## Programmatically Retrieve Data

If you need an API endpoint to integrate this data to your own application/ project, that is also possible. A "GET" endpoint is enabled for retrieving data through query parameters for river, river basin, dates and gauging station. 

__Steps__

1. First you need to have Docker on your machine.
2. Next up go to the `image/` directory.
3. Build the docker image from Dockerfile available: 
```bash
docker build -t flood-api:latest .
```
4. Now run the image
```bash
docker run -p 9999:9999 flood-api:latest
```

To run in the detached mode:
```bash
docker run -d -p 9999:9999 flood-api:latest
```

On your web browser, head to the `http://0.0.0.0:9999/docs` to interact with the API responses using the Swagger UI.

### Example use case
```python
import requests

base_url = "http://0.0.0.0:9999/water-level/"

def main(base_url: str, params_i: dict):
    res = requests.get(base_url, params=params_i)
    if res.ok:
        print(res.status_code)
        return res.json()


if __name__ == "__main__":
    parameters = {
    "date": "20251130",
    "river": "Gurugoda Oya"
    }
    res = main(base_url, parameters)
    print(res)
```


## Future Work

Currently the focus is on providing a clean dataset for analytical purposes which at the base level, it excels at. However, for a more comprehensive, easy accessible for everyone, the following future work are to be carried out:

- Extraction of PDF report data through IO bytes rather than storing in local storage.
- Storing raw json converted report data (before deriving further attributes) in MongoDB documents for persistance.
- A click group for batch backfilling and target backfilling extraction of data.
- A plotly dash app for dynamically filtering data through a UI and downloading to CSV formats which provides access to everyone without needing specific technical knowledge.
- Deploying to Digitial Ocean droplet and enable ochestration for productionizing.