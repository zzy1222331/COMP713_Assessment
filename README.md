# COMP713 Assessment 2 - Option B: P2P Message-Passing System

## 1. Software and Tools Required
- Python 3.8+ (Tested on Python 3.11.9)
- No external libraries required (uses standard `socket`, `threading`, `json`, `sys`, `time` modules)

## 2. Installation or Setup Instructions
1. Ensure Python 3 is installed and configured in your system PATH.
2. Download or clone this repository to your local machine.

## 3. Instructions for Starting the System
Open three separate terminal windows (e.g., PowerShell or CMD) in the project directory and run the following commands exactly as shown:
- **Terminal 1 (Peer 1):** `python peer.py 1 5001 2:localhost:5002 3:localhost:5003`
- **Terminal 2 (Peer 2):** `python peer.py 2 5002 1:localhost:5001 3:localhost:5003`
- **Terminal 3 (Peer 3):** `python peer.py 3 5003 1:localhost:5001 2:localhost:5002`

## 4. Instructions for Testing the Main Functions
- **Chat & Vector Clock:** In `Peer 1>` prompt, type `chat Hello everyone`. Observe the vector clock merging in Peer 2 and Peer 3 terminals.
- **Bully Election:** In `Peer 1>` prompt, type `election`. Observe Peer 3 (highest ID) becoming the new Leader and announcing to others.
- **Failure Handling (Timeout):** Type `quit` in `Peer 3>` (simulating a crash). Then type `election` in `Peer 1>`. Wait ~6 seconds for timeout. Peer 2 will become the new Leader.
- **Invalid Input:** Type `hello world`. The system will respond with "Unknown command."

## 5. Required Configuration Values / Ports
- Ports used: 5001, 5002, 5003.
- No database setup is required. All state is maintained in memory.
- No environment variables are required.

## 6. Known Limitations
- No persistent storage (messages and state are lost when the program exits).
- Heartbeat detects failures but does not automatically trigger an election (requires manual `election` command).
- Peer discovery is static (requires a predefined list of peers at startup).