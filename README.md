# Leaderboard System with MVCC, OCC, and Hybrid MVCC

This project demonstrates a leaderboard system implementing three concurrency control models: **Multi-Version Concurrency Control (MVCC)**, **Optimistic Concurrency Control (OCC)**, and a **Hybrid MVCC (MVocc)**. These models manage concurrent transactions, ensuring data consistency and handling conflicts effectively.

## Models Implemented

1. **MVCC (Multi-Version Concurrency Control)**:
   - Each transaction works with a snapshot of the data, allowing concurrent read and write operations without locking. When a transaction commits, it checks if the data was modified by another transaction and handles conflicts by versioning the records.

2. **OCC (Optimistic Concurrency Control)**:
   - Transactions assume no conflicts will occur. They perform operations optimistically, and only validate during the commit phase. If a conflict is detected, the transaction is rolled back and retried.

3. **Hybrid MVCC (MVocc)**:
   - This combines elements of both MVCC and OCC. It utilizes multi-versioned records for transactions, but also incorporates optimistic strategies to manage conflicts, offering a more flexible concurrency control approach.

## How to Run

1. **Install Dependencies**:
   Ensure you have Python 3.x and the required libraries (`pandas`, `threading`).

2. **Run the Scripts**:
   - To run **MVCC**:
     ```bash
     python mvcc_problem.py
     ```

   - To run **OCC**:
     ```bash
     python occ_problem.py
     ```

   - To run **Hybrid MVCC** (MVocc):
     ```bash
     python mvocc_problem.py
     ```

Each script demonstrates the respective concurrency control mechanism managing concurrent leaderboard updates.

## Features

- **Concurrency Management**: Efficient management of concurrent transactions using MVCC, OCC, and Hybrid MVCC.
- **Leaderboard Updates**: Simulate problem-solving submissions with concurrent score updates.
- **Versioning and Conflict Resolution**: Handle conflicts and ensure consistency across transactions.

---
