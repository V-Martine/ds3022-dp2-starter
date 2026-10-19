# DS 3022 - Project #2
### Victoria Martinez - October 19 - UVA id: yqg3zy 
### Submission README that explains my pipeline 

Prefect flow 'dp2-pipeline' in prefect-flow.py populates my SQS queue, collects the 21 delayed fragements (differing in the length of the delay between messages), lands them in DuckDB ('raw_fragements), runs 'dbt build' as a quality gate and then submits the assembled phrase to "dp2-submit' only if dbt passes. 

My pipeline: 
![DAG](docs/dag.jpeg)

'''
populate_queue -> monitor_queue -> collect_messages -> dbt_build -> read_phrase -> submit_solution
'''

## Setup and Run Instructions: 
1. Followed the SETUP.md to set up the venv and requirements: `python -m venv .venv && source .venv/bin/activate && pip install -r requirements.txt`
2. Set up the .env and added my 'UVA_ID' 
3. Followed the SETUP.md for the four terminals (SQS emulator, scatter API, Prefect server, working terminal: T1: 'moto_server -p 9324', 
T2: 'python kit/scatter_api.py', T3: 'python kit/smoke_test.py', and T4: 'prefect config set PREFECT_API_URL="http://127.0.0.1:4200/api"
prefect server start')
4. Made sure that `python kit/smoke_test.py` prints `Emulator OK`. 
5. Tested the submission from the **repo root**: `python prefect-flow.py`, then `python kit/check_submission.py`.

### Environment Variables: 
`UVA_ID`, `SCATTER_PORT` (default 8000), `DP2_DUCKDB` (default `dp2.duckdb`),
`DP2_FAST` (1 = 5-60 s delays, 0 = 30-900 s).

## My polling strategy: 
### Tasks in 'prefect-flow.py' 
1. 'monitor_queue': polls every 5 secs and logs the visible/not visible/delayed messages. Includes both a countdown for progress and a timeout.  
2. 'collect_messages': ensured that the target number to receive is 21 unique messages by cross-checking with the 'counts.values'. Kept both a tracker for inserting and existing to ensure that all a duplicate message not stored twice and deletes the message from the queue to prevent anything reprocessing. 
3. 'dbt_build': runs a check for the dbt pipeline and returns the same tests as running dbt by hand. 
4. 'read_phrase': checks if a phrase exists in the 'main.assembled_phrase' and reads it from the dbt mart model in DuckDB 
5. submit_solution: provides 2 retries in case it fails due to the wrong order or missing word. Returns the message attributes of UVA-ID, the phrase and the platform along with the HTTP status when submitting the solution. 



### Receive -> persist -> delete: 
For each of the messages, (1) read 'order_no' and 'word' from 'MessageAttributes' (2) INSERT into 'raw.fragements, and then (3) 'delete_message'. If the message crashes between the save/insert step and the delete step (steps 2 and 3), the message reappears after the visibility timeout and is a "repeat delivery". This means that the code skips the insert (same order_no and word) but still deletes it. But, if it crashes before the insert step, nothing is lost because the message is still on the queue. This ensures that a fraqgment is never deleted before it is safely stored. 
For example: INSERT EXAMPLE FROM SAME ORDER_NO

### dbt process and tests
The 'stg_fragments' trims and casts the 'order_no' to an integer and deduplicates all of the exact repeats (where 'order_no' and 'word' both match). For the uniqueness test: I added checks to ensure that both are non-null and that 'order_no' is unique. If there is a match, both rows remain in staging and the uniqueness test fails. 
The 'assert_complete_sequence' tests that the staging (1) contains 21 fragments, (2) starts at order 1, (3) ends at order 21 and (4) has no gaps. 
dbt then builds the 'assembled_phrase' only when all of the tests pass. 

## Git-Ignore:
`.env`, `*.duckdb`, `dbt/target/` and logs are git-ignored.

## Submission Messages: 
Passing submission:
(.venv) (base) papi@Victoria-MacBook-7 ds3022-dp2-starter % python kit/check_submission.py
PASS  yqg3zy / prefect  receipt: 126A1DEA8F

1/1 submission(s) correct. Checked submissions were removed.