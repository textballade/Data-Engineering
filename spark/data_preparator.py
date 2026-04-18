import click
import requests
from pathlib import Path
from datetime import datetime
from concurrent.futures import ThreadPoolExecutor

CSV_GZ_BASE_URL = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download"

def parse_range(str: str):
    dt = datetime.strptime(str, "%Y-%m-%d")
    return (dt.year, dt.month)

def create_dates_list(start, end):
    s_year, s_month = parse_range(start)
    e_year, e_month = parse_range(end)
    dates_list = []
    while (s_year, s_month) <= (e_year, e_month):
        dates_list.append({"year": s_year, "month": s_month})

        if s_month < 12:
            s_month += 1
        else:
            s_year += 1
            s_month = 1
    return dates_list

def prepare_dir(taxi_type, dir):
    p = Path(dir) / taxi_type
    if not p.is_absolute():
        base_dir = Path(__file__).parent
        p = base_dir / p
    p.mkdir(parents=True, exist_ok=True)
    return p


def download_file(args):
    taxi_type, date, dir = args
    filename = f"{taxi_type}_tripdata_{date["year"]}-{date["month"]:02d}.csv.gz"
    url = f"{CSV_GZ_BASE_URL}/{taxi_type}/{filename}"

    print(f"Downloading: {url}")
    with requests.get(url, stream=True, timeout=120) as response:
        if response.status_code == 404:
            print(f"404: {url}")
            return None
        
        response.raise_for_status()
        file_path = prepare_dir(taxi_type, dir) / filename
        
        with open(file_path, "wb") as f:
            for chunk in response.iter_content(chunk_size=1024 * 1024):
                if chunk:
                    f.write(chunk)



@click.command()
@click.option("--taxi", default=["yellow","green"], multiple=True, help="type of taxi")
@click.option("--start", default="2020-01-01", help="start of data")
@click.option("--end", default="2020-03-01", help="start of data")
@click.option("--dir", default="/tmp", help="dir for data")

def main(taxi, start, end, dir):
    dates_list = create_dates_list(start, end)
    tasks = [ (type, date, dir) for type in taxi for date in dates_list]
    with ThreadPoolExecutor(max_workers=4) as executor:
        list(executor.map(download_file, tasks))

if __name__ == "__main__":
    main()
