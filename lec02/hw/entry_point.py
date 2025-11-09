import threading

from bin.check_jobs import run_all_jobs
from job1.main import run as run_server_job1
from job2.main import run as run_server_job2

if __name__ == "__main__":
    # run concurrently both job1 and job2 Flask apps
    thread1 = threading.Thread(target=run_server_job1)
    thread2 = threading.Thread(target=run_server_job2)
    thread3 = threading.Thread(target=run_all_jobs)

    thread1.start()
    thread2.start()
    thread3.start()

    thread1.join()
    thread2.join()
    thread3.join()
