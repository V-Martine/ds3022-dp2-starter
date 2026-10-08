"""DS 3022 Data Project 2: Prefect flow.

Your pipeline must, without human intervention:
  1. POST to the scatter API to populate your queue (once per run, never inside a retry loop)
  2. Monitor the queue (get_queue_attributes) and collect all 21 messages
  3. Land each fragment in DuckDB (raw.fragments) BEFORE deleting its message
  4. Run dbt: tests are the quality gate, the mart model holds the phrase
  5. Submit the phrase to the 'dp2-submit' queue and log the HTTP status code

Run:   python prefect-flow.py
Then:  python kit/check_submission.py

Every task already has a header. Checkpoint A (Lesson 9): fill in populate_queue,
get_counts and monitor_queue. Lesson 10: collect_messages, dbt_build, read_phrase
and submit_solution. The tasks not called by the flow yet cannot fail, so you can
run the file at any point. The skeleton is a suggestion: rename, split or add
tasks as your design needs, and keep your DAG sketch in step with them.
Use Prefect's logger (get_run_logger) for logging. Never hard-code your
computing ID or the endpoint: read them from the environment (.env).
"""
# Victoria Martinez 
# October 8th, 2026 - DS 3022 
# last update: 

import os
import time

import boto3
from moto import logs
import requests
from dotenv import load_dotenv
from prefect import flow, task, get_run_logger

load_dotenv()

UVA_ID = os.environ["UVA_ID"]
SCATTER_URL = f"http://127.0.0.1:{os.environ.get('SCATTER_PORT', '8000')}/api/scatter/{UVA_ID}"
DUCKDB_PATH = os.environ.get("DP2_DUCKDB", "dp2.duckdb")

sqs = boto3.client("sqs")  # the endpoint comes from AWS_ENDPOINT_URL_SQS


@task
def populate_queue() -> str: # returns a string
    """POST to the scatter API and return your queue URL."""
    # TODO (Checkpoint A)
    # `populate_queue` POSTs to the scatter API and logs your queue URL
    
    logger = get_run_logger()
    response = requests.post(SCATTER_URL, timeout=30)
    response.raise_for_status() # raises an error if the server returns a bad status code
    payload = response.json() # parses the JSON response into a Python format

    queue_url = payload["sqs_url"] # extracts the SQS queue URL from the payload
    logger.info(f"Queue URL: {queue_url}") # records the URL in the task log 
    return queue_url

# _________________________________________________________
# `get_counts` and `monitor_queue` log the visible, 
# not visible and delayed counts until Delayed reaches 0

def get_counts(queue_url: str) -> dict: # returns a dictionary
    """Plain helper (not a task): return the three ApproximateNumberOf... counters as integers."""
    # TODO (Checkpoint A)
    response = sqs.get_queue_attributes(
        QueueUrl=queue_url, # specifies which queue to inspect
        AttributeNames=["ApproximateNumberOfMessages", 
                        "ApproximateNumberOfMessagesNotVisible",
                        "ApproximateNumberOfMessagesDelayed"
                    ]
    )
    attributes = response["Attributes"]   #extracts the correct attributes from the dictionary

    # makes sure that all of the retuns are integers and defaults to 0 if absent 
    return{
        "visible" : int(attributes.get("ApproximateNumberOfMessages", 0)), 
        "not_visible" : int(attributes.get("ApproximateNumberOfMessagesNotVisible", 0)), 
        "delayed" : int(attributes.get("ApproximateNumberOfMessagesDelayed", 0))
    }
        

@task
def monitor_queue(queue_url: str, poll_s: int = 5, timeout_s: int = 1200) -> dict:
    """Log the three counters every poll_s seconds until nothing is delayed; raise after timeout_s."""
    # TODO (Checkpoint A)
    logger = get_run_logger() 
    deadline = time.time() + timeout_s                  # sets a deadline for the monitoring loop, avoids infinite loops
    while True:                                         # conditional so that the loop continues until the delayed count is 0 or the timeout is reached
        counts = get_counts(queue_url)                  # retrieves the current counts from the queue
        logger.info(                                    # logs the current counts for visibility
            f"Queue counts: visible = %d, not visible = %d, delayed = %d", 
            counts["visible"], 
            counts["not_visible"], 
            counts["delayed"], 
            )         
        if counts["delayed"] == 0:                      # checks if there are no delayed messages left
            return counts                               # exits the loop if all messages are processed

        remaining = deadline - time.monotonic()         # calculates the remaining time until the timeout
        if remaining <= 0:                              # checks if the timeout has been reached
            raise TimeoutError(
                f"Queue still has {counts['delayed']} delayed messages after {timeout_s} seconds"
            )
        time.sleep(min(poll_s, remaining))                              # waits for poll_s seconds before checking again


@task
def collect_messages(queue_url: str, expected: int = 21) -> int:
    """Receive, land in DuckDB, then delete. Return how many fragments are stored."""
    # TODO (Checkpoint B): MessageAttributeNames=["All"] and resp.get("Messages", [])
    raise NotImplementedError


@task
def dbt_build() -> None:
    """Run `dbt build --project-dir dbt --profiles-dir dbt`; raise if anything fails."""
    # TODO (Checkpoint C)
    raise NotImplementedError


@task
def read_phrase() -> str:
    """Read the phrase from the dbt mart model in DuckDB."""
    # TODO (Checkpoint C)
    raise NotImplementedError


@task
def submit_solution(phrase: str, platform: str = "prefect") -> int:
    """Send to dp2-submit with uvaid / phrase / platform attributes. Return the HTTP status code."""
    # TODO (Checkpoint D)
    raise NotImplementedError


@flow(name="dp2-pipeline", log_prints=True)
def dp2_pipeline():
    queue_url = populate_queue()
    monitor_queue(queue_url)
    # TODO (Lesson 10): collect_messages -> dbt_build -> read_phrase -> submit_solution


if __name__ == "__main__":
    dp2_pipeline()
