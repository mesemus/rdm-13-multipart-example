from datetime import datetime
import os
from urllib.parse import urlparse

import requests
import typer
import warnings

records_url = "https://127.0.0.1:5000/api/records"

requests_extra = {
    'headers': {
       "Authorization": f"Bearer {os.environ['RDM_TOKEN']}",
    },
    'verify': False
}


def remote_upload(uri: str = "https://zenodo.org/records/13253155/files/2.pdf?download=1"):
    uri += f"&ts={datetime.now().timestamp()}"  # single uri can be associated only once - there is unique constraint on file uri
    key = urlparse(uri).path.split("/")[-1]
    rec = requests.post(records_url, json={
        "metadata": {
            "title": f"Remote upload @ {datetime.now()}"
        }
    }, **requests_extra).json()

    file_rec = requests.post(
        rec["links"]["files"],
        json=[{
            "key": key,
            "transfer_type": "R",
            "uri": uri
        }],
        **requests_extra
    ).json()["entries"][0]

    requests.post(file_rec["links"]["commit"], **requests_extra)

    print(f"Record created, go to https://127.0.0.1:5000/uploads/{rec['id']}")


if __name__ == "__main__":
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        typer.run(remote_upload)