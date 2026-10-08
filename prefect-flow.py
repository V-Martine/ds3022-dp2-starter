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
import duckdb
from moto import connect, logs
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


@task # added a timeout limit to the skeleton def 
def collect_messages(queue_url: str, expected: int = 21, timeout_s: int = 1200) -> int:
    """Receive, land in DuckDB, then delete. Return how many fragments are stored."""
    # TODO (Checkpoint B): MessageAttributeNames=["All"] and resp.get("Messages", [])
    logger = get_run_logger()
    deadline = time.monotonic() + timeout_s
    stored = 0 # keeps a running counter of the stored messages 

    con = duckdb.connect(DUCKDB_PATH) # establishes a connection to the DuckDB database
    try: 
        con.execute("CREATE SCHEMA IF NOT EXISTS raw") # creates the schema if it doesn't exist
        con.execute("""
            CREATE TABLE IF NOT EXISTS raw.fragments (
                order_no VARCHAR, 
                word VARCHAR,
                received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)

        while True: 
            counts = get_counts(queue_url) # retrieves the current counts from the queue

            # finish once we've hit all 21 stored messages 
            # need to ensure that the messages are also unique and not duplicates 
            if stored >= 21 and all(value == 0 for value in counts.values()): # checks if there are any visible messages left
                logger.info("Collection complete: stored =%d, counts=%s", stored, counts)
                return {"stored": stored, "counts": counts} # returns the final counts and the number of stored messages

            if time.monotonic() >= deadline: # checks if the timeout has been reached
                raise TimeoutError(
                f"Collection timed out: stored {stored}/21 fragments"
                f"queue counts = {counts} after {timeout_s} seconds"
                )

            response = sqs.receive_message(
                QueueUrl=queue_url,
                MaxNumberOfMessages=10, # retrieves up to 10 messages at a time
                WaitTimeSeconds=5, # long polling for 5 seconds
                MessageAttributeNames=["All"] # retrieves all message attributes
            )

            # SQS may omit the "Messages" key if there are no messages available
            for msg in response.get("Messages", []): # iterates over the received messages
                atts = msg["MessageAttributes"] # extracts the message attributes
                order_no = atts["order_no"]["StringValue"] # extracts the order number from the message attributes
                word = atts["word"]["StringValue"] # extracts the word from the message attributes

                existing = con.execute(
                    "SELECT word FROM raw.fragments WHERE order_no = ?",
                    [order_no],
                ).fetchone() # checks if the message has already been stored in the database

                if not existing: # only insert the message if it hasn't been stored already
                    con.execute(
                        """
                        INSERT INTO raw.fragments (order_no, word, recieved_at) \
                        VALUES (?, ?, current_timestamp),
                        """ 
                        [order_no, word] # inserts the order number and word into the DuckDB table
                    )
                elif existing[0] != word: # checks if the existing word is different from the new word
                    # needed to prevent duplicates!! 
                    raise ValueError(
                        f"Conflicting words for order_no {order_no}: existing={existing[0]}, new={word}"
                    )
                else: 
                    logger.info("Repeat delivery for order_no=%s; skipping insert")

                sqs.delete_message( # deletes the message from the queue to prevent reprocessing
                    QueueUrl=queue_url,
                    ReceiptHandle=msg["ReceiptHandle"]
                )

            stored += 1 # increments the stored counter
            logger.info("Stored and deleted fragment: order_no=%s, word=%s", order_no, word) # logs the stored fragment details

    except Exception as e:
        logger.error(f"Error occurred while executing DuckDB commands: {e}")
        raise
    finally:
        con.close()

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
