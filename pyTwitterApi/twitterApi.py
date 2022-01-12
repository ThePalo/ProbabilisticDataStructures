import requests
import os
import json
import csv
import pandas as pd

# To set your environment variables in your terminal run the following line:
# export 'BEARER_TOKEN'='<your_bearer_token>'
bearer_token = os.environ.get("BEARER_TOKEN")


def create_url():
    return "https://api.twitter.com/2/tweets/sample/stream?expansions=author_id"


def bearer_oauth(r):
    """
    Method required by bearer token authentication.
    """

    r.headers["Authorization"] = f"Bearer {bearer_token}"
    r.headers["User-Agent"] = "v2SampledStreamPython"
    return r


def connect_to_endpoint(url, writer):
    response = requests.request("GET", url, auth=bearer_oauth, stream=True)
    print(response.status_code)
    for response_line in response.iter_lines():
        if response_line:
            js = json.loads(response_line)
            username = js["includes"]["users"][0]["username"]
            writer.writerow([username])
    if response.status_code != 200:
        raise Exception(
            "Request returned an error: {} {}".format(
                response.status_code, response.text
            )
        )


def cleanDataset():
    file_name = "dataset.csv"
    file_name_output = "dataset.csv"

    df = pd.read_csv(file_name, sep="\t")
    # Notes:
    # - the `subset=None` means that every column is used
    #    to determine if two rows are different; to change that specify
    #    the columns as an array
    # - the `inplace=True` means that the data structure is changed and
    #   the duplicate rows are gone
    df.drop_duplicates(subset=None, inplace=True)

    # Write the results to a different file
    df.to_csv(file_name_output, index=False)

def getData():
    f = open('dataset.csv', 'a')
    writer = csv.writer(f)

    url = create_url()
    timeout = 0
    while timeout < 250000:
        connect_to_endpoint(url, writer)
        timeout += 1
    f.close()

def main():
    #getData()
    cleanDataset()


if __name__ == "__main__":
    main()
