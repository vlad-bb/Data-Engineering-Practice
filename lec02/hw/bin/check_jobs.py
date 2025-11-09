import os
import sys
import time

import requests

JOB1_PORT = 8081
JOB2_PORT = 8082

RAW_DIR = os.path.join("raw", "sales", "2022-08-09")
STG_DIR = os.path.join("stg", "sales", "2022-08-09")


def run_job1():
    print("Starting job1:")
    resp = requests.post(
        url=f"http://localhost:{JOB1_PORT}/",
        json={"date": "2022-08-09", "raw_dir": RAW_DIR},
    )
    assert resp.status_code == 201
    print("job1 completed!")


def run_job2():
    print("Starting job2:")
    resp = requests.post(
        url=f"http://localhost:{JOB2_PORT}/",
        json={"raw_dir": RAW_DIR, "stg_dir": STG_DIR},
    )
    assert resp.status_code == 201
    print("job2 completed!")


def run_all_jobs():
    run_job1()
    time.sleep(3)
    run_job2()
    print("All jobs completed!")
    sys.exit(0)
