import requests
import os
import json
import csv

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


def main():
    f = open('dataset.csv', 'a')
    writer = csv.writer(f)

    url = create_url()
    timeout = 0
    while timeout < 40000:
        connect_to_endpoint(url, writer)
        timeout += 1
    f.close()


if __name__ == "__main__":
    main()
