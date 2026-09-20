# COMP713 Assessment 2 - Option B: P2P Message-Passing System
**Student Name:** [Zhiyuan Zhang] | **Student ID:** [22170060]

## 1. Software and Tools Required
- Python 3.8+ (Tested on Python 3.11.9)
- No external libraries required (uses standard `socket`, `threading`, `json`, `sys`, `time` modules)
- OS: Windows / Linux / macOS (Tested on Windows 10)

## 2. Installation or Setup Instructions
1. Ensure Python 3 is installed and configured in your system PATH.
2. Download or clone this repository to your local machine.
3. No database setup is required. All state is maintained in memory.

## 3. Instructions for Starting the System
Open **three separate terminal windows** (e.g., PowerShell or CMD) in the project directory and run the following commands exactly as shown:
- **Terminal 1 (Peer 1):** `python -u peer.py 1 5001 2:localhost:5002 3:localhost:5003`
- **Terminal 2 (Peer 2):** `python -u peer.py 2 5002 1:localhost:5001 3:localhost:5003`
- **Terminal 3 (Peer 3):** `python -u peer.py 3 5003 1:localhost:5001 2:localhost:5002`

*(Note: The `-u` flag is used to ensure real-time log flushing on Windows CMD.)*

## 4. Instructions for Testing the Main Functions
Once all three peers are running, use the CLI commands:
- **Chat & Vector Clock:** Type `chat Hello everyone` in Peer 1. Observe the vector clock merging in Peer 2 and Peer 3 terminals.
- **Bully Election:** Type `election` in Peer 1. Observe Peer 3 (highest ID) becoming the Leader, and Peer 1 & 2 receiving the `New LEADER announced: Peer 3` message.
- **Failure Handling (Timeout):** Type `quit` in Peer 3 (simulating a crash). Then type `election` in Peer 1. Wait for the 4-second timeout countdown. Peer 1 will become the new Leader.
- **Invalid Input:** Type `hello world`. The system will respond with `Unknown command. Available: chat, clock, election, leader, status, quit`.

## 5. Required Configuration Values / Ports
- Ports used: 5001 (Peer 1), 5002 (Peer 2), 5003 (Peer 3).
- Peer addresses are passed as command-line arguments (e.g., `2:localhost:5002`).
- No environment variables or database credentials are required.

## 6. Known Limitations
- **Static Discovery:** Peer discovery is static and requires the addresses to be passed at startup. Dynamic peer joining is not implemented.
- **No Persistence:** Messages and state (Vector Clocks, Leader ID) are stored in memory and are lost if the process exits.
- **Heartbeat:** The heartbeat runs every 5 seconds, but it currently only logs liveness and does not automatically trigger an election (manual `election` command is required).