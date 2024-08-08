import os
import warnings
from datetime import datetime
from pathlib import Path

import requests
import typer

records_url = "https://127.0.0.1:5000/api/records"

requests_extra = {
    "headers": {
        "Authorization": f"Bearer {os.environ['RDM_TOKEN']}",
    },
    "verify": False,
}


def multipart_upload(fname: Path, parts: int = 1):
    file_size = fname.stat().st_size

    rec = requests.post(
        records_url,
        json={"metadata": {"title": f"Multipart upload @ {datetime.now()}"}},
        **requests_extra,
    ).json()

    file_rec = requests.post(
        rec["links"]["files"],
        json=[
            {"key": fname.name, "transfer_type": "M", "parts": parts, "size": file_size}
        ],
        **requests_extra,
    ).json()["entries"][0]

    part_size = file_size // parts
    for part in range(parts):
        # do not use in production code :)

        # note: no requests_extra here, as going directly to s3
        if part < parts - 1:
            requests.put(
                file_rec["links"]["parts"][part]["url"],
                data=fname.read_bytes()[part * part_size : (part + 1) * part_size],
            )
        else:
            requests.put(
                file_rec["links"]["parts"][part]["url"],
                data=fname.read_bytes()[part * part_size :],
            )

    requests.post(file_rec["links"]["commit"], **requests_extra)

    print(f"Record created, go to https://127.0.0.1:5000/uploads/{rec['id']}")


if __name__ == "__main__":
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        typer.run(multipart_upload)
