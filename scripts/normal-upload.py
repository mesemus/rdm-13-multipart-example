from datetime import datetime
import os
from pathlib import Path
import warnings

import requests
import typer

records_url = "https://127.0.0.1:5000/api/records"

requests_extra = {
    'headers': {
       "Authorization": f"Bearer {os.environ['RDM_TOKEN']}",
    },
    'verify': False
}


def normal_upload(fname: Path):
    rec = requests.post(records_url, json={
        "metadata": {
            "title": f"Normal upload @ {datetime.now()}"
        }
    }, **requests_extra).json()

    file_rec = requests.post(
        rec["links"]["files"],
        json=[{
            "key": fname.name,
        }],
        **requests_extra
    ).json()["entries"][0]

    requests.put(file_rec["links"]["content"], data=fname.read_bytes(), **requests_extra)

    requests.post(file_rec["links"]["commit"], **requests_extra)

    print(f"Record created, go to https://127.0.0.1:5000/uploads/{rec['id']}")


if __name__ == "__main__":
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        typer.run(normal_upload)