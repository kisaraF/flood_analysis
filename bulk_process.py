import subprocess
import os
from Logger import init_logger as lg


def get_pdf_list() -> list:
    pdf_ls = os.listdir(f"{os.getcwd()}/Data/PDF/")
    return pdf_ls


def get_latest_pdf(pdf_ls: list) -> str:
    return max([i.split("__")[-1].split(".")[0] for i in pdf_ls])


def main():
    pdf_ls = get_pdf_list()
    print(pdf_ls)

    for i in range(0, len(pdf_ls)):
        pdf_ls_sub = get_pdf_list()
        epoch = get_latest_pdf(pdf_ls_sub)
        pdf_name = [i for i in pdf_ls_sub if epoch in i][0]
        subprocess.run(["python3", "extract_pdf.py"])
        json_file = [i for i in os.listdir(f"{os.getcwd()}/Data/JSON") if epoch in i][
            -1
        ]
        data_pdf_folder = f"{os.getcwd()}/Data/PDF/{pdf_name}"
        data_json_folder = f"{os.getcwd()}/Data/JSON/{json_file}"
        destination_pdf = f"{os.getcwd()}/Data/Archive/PDF/"
        destination_json = f"{os.getcwd()}/Data/Archive/JSON/"
        subprocess.run(["python3", "parse_json.py"])
        subprocess.run(["mv", f"{data_pdf_folder}", f"{destination_pdf}"])
        subprocess.run(["mv", f"{data_json_folder}", f"{destination_json}"])


if __name__ == "__main__":
    main()
