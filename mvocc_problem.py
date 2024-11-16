import threading
import time
import pandas as pd
from collections import defaultdict
from datetime import datetime

class MVOCC:
    def __init__(self):
        self.records = defaultdict(list)
        self.transactions = {}
        self.next_tid = 1
        self.commit_log = []

    def new_transaction(self):
        tid = self.next_tid
        self.next_tid += 1
        self.transactions[tid] = {
            "read_set": [],
            "write_set": [],
            "begin_ts": len(self.commit_log),
            "snapshot": {}
        }
        return tid

    def read(self, tid, key):
        latest_version = None
        for version in reversed(self.records[key]):
            if version['begin_ts'] <= self.transactions[tid]["begin_ts"] and \
               (version['end_ts'] is None or version['end_ts'] > self.transactions[tid]["begin_ts"]):
                latest_version = version
                break

        if latest_version:
            self.transactions[tid]["snapshot"][key] = latest_version
            self.transactions[tid]["read_set"].append((key, latest_version))
            return latest_version['value'].copy()
        return None

    def write(self, tid, key, value):
        self.transactions[tid]["write_set"].append((key, value))

    def validate(self, tid):
        current_ts = len(self.commit_log)

        for key, read_version in self.transactions[tid]["read_set"]:
            current_versions = self.records[key]
            if not current_versions:
                continue

            latest_version = current_versions[-1]
            if latest_version['begin_ts'] > read_version['begin_ts']:
                print(f"Transaction {tid} validation failed: newer version exists for {key}")
                return False

            if read_version['end_ts'] is not None and read_version['end_ts'] <= current_ts:
                print(f"Transaction {tid} validation failed: read version no longer valid for {key}")
                return False

        return True

    def commit(self, tid):
        if not self.validate(tid):
            return False

        commit_ts = len(self.commit_log)

        for key, value in self.transactions[tid]["write_set"]:
            if self.records[key]:
                self.records[key][-1]['end_ts'] = commit_ts

            new_version = {
                "value": value.copy(),
                "begin_ts": commit_ts,
                "end_ts": None,
                "tid": tid
            }
            self.records[key].append(new_version)

        self.commit_log.append(tid)
        return True

def load_initial_data(mvcc, df):
    """Load initial leaderboard data into the system"""
    for idx, row in df.iterrows():
        tid = mvcc.new_transaction()
        key = f"user_{row['UserID']}"
        mvcc.write(tid, key, row.to_dict())
        if mvcc.commit(tid):
            print(f"Loaded initial data for user {row['UserID']}")
    print("Initial leaderboard loading complete")

def submit_problem_transaction(mvcc, user_id, score_increase, sleep_time=0):
    """Submit a problem solution and update the leaderboard"""
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        tid = mvcc.new_transaction()
        key = f"user_{user_id}"

        print(f"\nTransaction {tid} started: Updating score for user {user_id}")

        current_data = mvcc.read(tid, key)
        if current_data is None:
            print(f"Transaction {tid}: User {user_id} not found")
            return

        current_score = current_data['Score']
        print(f"Transaction {tid} read current score: {current_score}")

        time.sleep(sleep_time)

        new_data = current_data.copy()
        new_data['Score'] += score_increase
        new_data['LastSubmission'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        mvcc.write(tid, key, new_data)

        if mvcc.commit(tid):
            print(f"Transaction {tid} successfully updated score from {current_score} to {new_data['Score']}")
            return True
        else:
            print(f"Transaction {tid} failed, attempt {retry_count + 1} of {max_retries}")
            retry_count += 1
            time.sleep(0.1)

    print(f"Transaction {tid} failed after {max_retries} attempts")
    return False

def print_leaderboard(mvocc):
    """Print the current leaderboard"""
    print("\nCurrent Leaderboard:")
    all_users = [key for key in mvocc.records if key.startswith("user_")]
    leaderboard = []

    for user_key in all_users:
        latest_version = mvocc.records[user_key][-1]['value']
        leaderboard.append(latest_version)

    leaderboard.sort(key=lambda x: x['Score'], reverse=True)
    for rank, user in enumerate(leaderboard, 1):
        print(f"Rank {rank}: User {user['UserID']} - Score {user['Score']}")

def load_sample_data():
    """Create sample leaderboard data"""
    sample_data = {
        'UserID': range(1, 6),
        'Score': [100, 200, 150, 180, 120],
        'LastSubmission': ['2024-01-01'] * 5
    }
    return pd.DataFrame(sample_data)

def run_concurrent_submissions(mvocc):
    """Run multiple concurrent problem submissions"""
    threads = []

    # Define concurrent updates
    updates = [
        (submit_problem_transaction, (mvocc, 1, 50, 2)),
        (submit_problem_transaction, (mvocc, 2, 30, 1)),
        (submit_problem_transaction, (mvocc, 3, 70, 1.5)),
        (submit_problem_transaction, (mvocc, 1, -20, 0.5))
    ]

    # Start all threads
    for func, args in updates:
        thread = threading.Thread(target=func, args=args)
        threads.append(thread)
        thread.start()

    # Wait for all threads to complete
    for thread in threads:
        thread.join()

def main():
    # Initialize system
    df = load_sample_data()
    mvocc = MVOCC()

    # Load initial data
    load_initial_data(mvocc, df)

    # Run concurrent submissions
    run_concurrent_submissions(mvocc)

    # Print final leaderboard
    print_leaderboard(mvocc)

if __name__ == "__main__":
    main()
