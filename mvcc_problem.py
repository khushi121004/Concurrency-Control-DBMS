import threading
import time
import pandas as pd
from collections import defaultdict
from datetime import datetime

class MVOCC:
    def __init__(self):
        self.records = defaultdict(list)  # For holding all versions of keys
        self.transactions = {}  # To track active transactions
        self.next_tid = 1
        self.commit_log = []

    def new_transaction(self):
        tid = self.next_tid
        self.next_tid += 1
        self.transactions[tid] = {
            "read_set": {},  # Keep track of what was read by this transaction
            "write_set": [],  # Keep track of what will be written
            "begin_ts": len(self.commit_log),  # Transaction start timestamp
            "snapshot": {}  # To hold the snapshot of the read values
        }
        return tid

    def read(self, tid, key):
        """Read with snapshot isolation"""
        latest_version = None
        # Look for the latest version of the key before the transaction start timestamp
        for version in reversed(self.records[key]):
            if version['begin_ts'] <= self.transactions[tid]["begin_ts"] and \
               (version['end_ts'] is None or version['end_ts'] > self.transactions[tid]["begin_ts"]):
                latest_version = version
                break

        if latest_version:
            self.transactions[tid]["snapshot"][key] = latest_version
            self.transactions[tid]["read_set"][key] = latest_version  # Record read version
            return latest_version['value'].copy()
        return None

    def write(self, tid, key, value):
        """Write to the transaction's write set"""
        self.transactions[tid]["write_set"].append((key, value))

    def commit(self, tid):
        """Commit the transaction, ensuring snapshot isolation"""
        commit_ts = len(self.commit_log)

        # Check for conflicts with the transaction's read set
        for key, old_version in self.transactions[tid]["read_set"].items():
            latest_version = self.records[key][-1] if self.records[key] else None
            if latest_version and latest_version != old_version:
                print(f"Transaction {tid} conflicted on {key}, retrying...")
                return False  # Conflict detected, abort commit

        # Update records with the write set
        for key, value in self.transactions[tid]["write_set"]:
            if self.records[key]:
                self.records[key][-1]['end_ts'] = commit_ts  # Close previous version if exists

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

def print_leaderboard(mvcc):
    """Print the current leaderboard"""
    print("\nCurrent Leaderboard:")
    all_users = [key for key in mvcc.records if key.startswith("user_")]
    leaderboard = []

    for user_key in all_users:
        latest_version = mvcc.records[user_key][-1]['value']
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

def run_concurrent_submissions(mvcc):
    """Run multiple concurrent problem submissions"""
    threads = []

    # Define concurrent updates
    updates = [
        (submit_problem_transaction, (mvcc, 1, 50, 2)),
        (submit_problem_transaction, (mvcc, 2, 30, 1)),
        (submit_problem_transaction, (mvcc, 3, 70, 1.5)),
        (submit_problem_transaction, (mvcc, 1, -20, 0.5))
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
    mvcc = MVOCC()

    # Load initial data
    load_initial_data(mvcc, df)

    # Run concurrent submissions
    run_concurrent_submissions(mvcc)

    # Print final leaderboard
    print_leaderboard(mvcc)

if __name__ == "__main__":
    main()
