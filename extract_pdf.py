import camelot
import os
from Logger import init_logger as lg


def get_pdf_list() -> list:
    pdf_ls = os.listdir(f"{os.getcwd()}/Data/PDF/")
    return pdf_ls


def get_latest_pdf(pdf_ls: list) -> str:
    return max([i.split("__")[-1].split(".")[0] for i in pdf_ls])


def pdf_to_json(file: str, epoch: str) -> None:
    status_mg = None
    table_ls = camelot.read_pdf(file)
    if len(table_ls) == 1:
        table_ls[0].to_json(f"{os.getcwd()}/Data/JSON/water_level_{epoch}.json")
        status_mg = (
            f'PDF table read and written to a JSON as "water_level_{epoch}.json"'
        )
    else:
        status_mg = "Error! More than 1 table identified"

    lg.logging.info(status_mg)


def main():
    log_type = "extract_pdf"
    lg.init_logger(log_type)
    pdf_ls = get_pdf_list()
    latest_report_epoch = get_latest_pdf(pdf_ls)
    pdf_item = [i for i in get_pdf_list() if latest_report_epoch in i][0]
    pdf_url = f"{os.getcwd()}/Data/PDF/{pdf_item}"
    pdf_to_json(pdf_url, latest_report_epoch)


if __name__ == "__main__":
    main()
