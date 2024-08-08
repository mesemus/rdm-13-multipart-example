import os
import time
import warnings
from datetime import datetime
from urllib.parse import urlparse

import requests
import typer

records_url = "https://127.0.0.1:5000/api/records"

requests_extra = {
    "headers": {
        "Authorization": f"Bearer {os.environ['RDM_TOKEN']}",
    },
    "verify": False,
}


def fetch_upload(
    uri: str = "https://zenodo.org/records/13253155/files/2.pdf?download=1",
):
    key = urlparse(uri).path.split("/")[-1]
    rec = requests.post(
        records_url,
        json={"metadata": {"title": f"Fetch upload @ {datetime.now()}"}},
        **requests_extra,
    ).json()

    file_rec = requests.post(
        rec["links"]["files"],
        json=[{"key": key, "transfer_type": "F", "uri": uri}],
        **requests_extra,
    ).json()["entries"][0]

    # wait for the fetch to get the data before finalizing the upload ...
    print("Waiting for the fetch to complete ", end="", flush=True)
    for _ in range(100):
        print(".", end="", flush=True)
        time.sleep(1)
        file_rec = requests.get(rec["links"]["files"], **requests_extra).json()[
            "entries"
        ][0]
        if file_rec["status"] == "completed":
            break
    else:
        raise Exception("Fetch upload did not complete")
    print()

    requests.post(file_rec["links"]["commit"], **requests_extra)

    print(f"Record created, go to https://127.0.0.1:5000/uploads/{rec['id']}")


if __name__ == "__main__":
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        typer.run(fetch_upload)
