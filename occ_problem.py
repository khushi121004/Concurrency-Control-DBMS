import threading
import time
import pandas as pd
from collections import defaultdict
from datetime import datetime

class OCC:
    def __init__(self):
        self.records = {}
        self.transactions = {}
        self.next_tid = 1
        self.commit_log = []

    def new_transaction(self):
        tid = self.next_tid
        self.next_tid += 1
        self.transactions[tid] = {
            "read_set": {},
            "write_set": {},
            "begin_ts": len(self.commit_log)
        }
        return tid

    def read(self, tid, key):
        if key in self.records:
            self.transactions[tid]["read_set"][key] = self.records[key]
            return self.records[key].copy()
        return None

    def write(self, tid, key, value):
        self.transactions[tid]["write_set"][key] = value

    def validate(self, tid):
        current_ts = len(self.commit_log)
        for key, version in self.transactions[tid]["read_set"].items():
            # Ensure no changes occurred since the transaction began
            if key not in self.records or self.records[key] != version:
                print(f"Transaction {tid} validation failed: key {key} has been modified")
                return False
        return True

    def commit(self, tid):
        if not self.validate(tid):
            return False

        # Apply all writes
        for key, value in self.transactions[tid]["write_set"].items():
            self.records[key] = value

        self.commit_log.append(tid)
        return True

def load_initial_data(occ, df):
    """Load initial leaderboard data into the system"""
    for idx, row in df.iterrows():
        tid = occ.new_transaction()
        key = f"user_{row['UserID']}"
        occ.write(tid, key, row.to_dict())
        if occ.commit(tid):
            print(f"Loaded initial data for user {row['UserID']}")
    print("Initial leaderboard loading complete")

def submit_problem_transaction(occ, user_id, score_increase, sleep_time=0):
    """Submit a problem solution and update the leaderboard"""
    max_retries = 3
    retry_count = 0

    while retry_count < max_retries:
        tid = occ.new_transaction()
        key = f"user_{user_id}"

        print(f"\nTransaction {tid} started: Updating score for user {user_id}")

        current_data = occ.read(tid, key)
        if current_data is None:
            print(f"Transaction {tid}: User {user_id} not found")
            return

        current_score = current_data['Score']
        print(f"Transaction {tid} read current score: {current_score}")

        time.sleep(sleep_time)

        new_data = current_data.copy()
        new_data['Score'] += score_increase
        new_data['LastSubmission'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

        occ.write(tid, key, new_data)

        if occ.commit(tid):
            print(f"Transaction {tid} successfully updated score from {current_score} to {new_data['Score']}")
            return True
        else:
            print(f"Transaction {tid} failed, attempt {retry_count + 1} of {max_retries}")
            retry_count += 1
            time.sleep(0.1)

    print(f"Transaction {tid} failed after {max_retries} attempts")
    return False

def print_leaderboard(occ):
    """Print the current leaderboard"""
    print("\nCurrent Leaderboard:")
    leaderboard = list(occ.records.values())
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

def run_concurrent_submissions(occ):
    """Run multiple concurrent problem submissions"""
    threads = []

    # Define concurrent updates
    updates = [
        (submit_problem_transaction, (occ, 1, 50, 2)),
        (submit_problem_transaction, (occ, 2, 30, 1)),
        (submit_problem_transaction, (occ, 3, 70, 1.5)),
        (submit_problem_transaction, (occ, 1, -20, 0.5))
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
    occ = OCC()

    # Load initial data
    load_initial_data(occ, df)

    # Run concurrent submissions
    run_concurrent_submissions(occ)

    # Print final leaderboard
    print_leaderboard(occ)

if __name__ == "__main__":
    main()
