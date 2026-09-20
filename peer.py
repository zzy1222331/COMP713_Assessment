#!/usr/bin/env python3
"""
COMP713 Assessment- Option B
P2P System -Final Version
"""

import socket
import threading
import json
import sys
import time

class Peer:
    def __init__(self, node_id, port, peers):
        self.node_id = node_id
        self.port = port
        self.peers = peers
        
        all_ids = [node_id] + list(peers.keys())
        self.vector_clock = {i: 0 for i in all_ids}
        
        self.leader_id = None
        self.in_election = False
        self.ok_received = set()
        
        self.running = True
        self.lock = threading.RLock()
        
        self.log(f"[Peer {self.node_id}] Initialized. Port: {self.port} | Peers: {list(self.peers.keys())}")
        self.log(f"[Peer {self.node_id}] Initial Vector Clock: {self.vector_clock}")

    def log(self, message):
        print(message, flush=True)

    def start(self):
        server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        server.bind(('0.0.0.0', self.port))
        server.listen(5)
        
        threading.Thread(target=self.accept_connections, args=(server,), daemon=True).start()
        threading.Thread(target=self.heartbeat_loop, daemon=True).start()
        self.command_loop()

    def accept_connections(self, server):
        while self.running:
            try:
                client, addr = server.accept()
                data = client.recv(4096)
                if data:
                    msg = json.loads(data.decode())
                    threading.Thread(target=self.handle_message, args=(msg,), daemon=True).start()
                client.close()
            except Exception:
                pass

    def send_message(self, target_id, msg):
        if target_id not in self.peers: return
        host, port = self.peers[target_id]
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.settimeout(5)
            s.connect((host, port))
            s.send(json.dumps(msg).encode())
            s.close()
        except Exception:
            pass

    def async_send(self, target_id, msg):
        threading.Thread(target=self.send_message, args=(target_id, msg), daemon=True).start()

    def handle_message(self, msg):
        msg_type = msg.get('type')
        sender = msg.get('sender')
        
        with self.lock:
            if 'clock' in msg:
                for k, v in msg['clock'].items():
                    k = int(k)
                    self.vector_clock[k] = max(self.vector_clock.get(k, 0), v)
            self.vector_clock[self.node_id] += 1
        
        if msg_type == 'CHAT':
            self.log(f"\n[Peer {self.node_id}] Received CHAT from Peer {sender}: '{msg.get('content')}'")
            self.log(f"  Sender Clock: {msg.get('clock')} | Local Clock: {self.vector_clock}")
        
        elif msg_type == 'ELECTION':
            self.log(f"[Peer {self.node_id}] Received ELECTION from Peer {sender}")
            self.async_send(sender, {'type': 'OK', 'sender': self.node_id, 'clock': dict(self.vector_clock)})
            
            if self.leader_id == self.node_id:
                self.log(f"[Peer {self.node_id}] I am already the Leader. Resending LEADER to Peer {sender}.")
                self.async_send(sender, {'type': 'LEADER', 'sender': self.node_id, 
                                         'leader': self.node_id, 'clock': dict(self.vector_clock)})
                return
                
            if self.node_id > msg.get('candidate', 0):
                threading.Thread(target=self.start_election, daemon=True).start()
        
        elif msg_type == 'OK':
            # Only track OK from higher ID peers
            if sender > self.node_id:
                with self.lock:
                    self.ok_received.add(sender)
        
        elif msg_type == 'LEADER':
            new_leader = msg.get('leader')
            with self.lock:
                self.in_election = False
                self.ok_received.clear()
                if self.leader_id != new_leader:
                    self.leader_id = new_leader
                    self.log(f"[Peer {self.node_id}] New LEADER announced: Peer {self.leader_id}")
        
        elif msg_type == 'HEARTBEAT':
            pass

    def start_election(self):
        with self.lock:
            if self.in_election or self.leader_id == self.node_id: return
            self.in_election = True
            self.ok_received.clear()
        
        self.log(f"[Peer {self.node_id}] Starting election...")
        higher_peers = [pid for pid in self.peers if pid > self.node_id]
        
        if not higher_peers:
            self.become_leader(reason="highest_id")
            return
        
        for pid in higher_peers:
            self.async_send(pid, {
                'type': 'ELECTION', 'sender': self.node_id, 
                'candidate': self.node_id, 'clock': dict(self.vector_clock)
            })
        
        self.log(f"[Peer {self.node_id}] Waiting for OK from higher peers...")
        wait_time = 4
        start_time = time.time()
        last_printed = -1
        
        while time.time() - start_time < wait_time:
            with self.lock:
                if not self.in_election:
                    self.log(f"[Peer {self.node_id}] Election aborted. A LEADER already exists.")
                    return
                if self.ok_received:
                    self.log(f"[Peer {self.node_id}] Received OK. Waiting for LEADER announcement...")
                    break
            
            remaining = int(wait_time - (time.time() - start_time))
            if remaining != last_printed:
                last_printed = remaining
                self.log(f"[Peer {self.node_id}] Waiting for OK... {remaining}s left")
            time.sleep(0.5)
        
        if not self.ok_received:
            with self.lock:
                if self.in_election:
                    self.become_leader(reason="timeout_no_ok")
            return

        leader_wait_time = 4
        leader_start = time.time()
        last_printed_leader = -1
        
        while time.time() - leader_start < leader_wait_time:
            with self.lock:
                if not self.in_election:
                    self.log(f"[Peer {self.node_id}] LEADER message received. Election complete.")
                    return
            remaining = int(leader_wait_time - (time.time() - leader_start))
            if remaining != last_printed_leader:
                last_printed_leader = remaining
                self.log(f"[Peer {self.node_id}] Waiting for LEADER announcement... {remaining}s left")
            time.sleep(0.5)
            
        with self.lock:
            if self.in_election:
                self.log(f"[Peer {self.node_id}] Timeout waiting for LEADER. Aborting to prevent split-brain.")
                self.in_election = False

    def become_leader(self, reason):
        with self.lock:
            if self.leader_id == self.node_id: return
            self.leader_id = self.node_id
            self.in_election = False
            
        if reason == "highest_id":
            self.log(f"[Peer {self.node_id}] I am the highest ID. Becoming LEADER.")
        else:
            self.log(f"[Peer {self.node_id}] No OK received (timeout). Becoming LEADER.")
            
        for pid in self.peers:
            self.async_send(pid, {
                'type': 'LEADER', 'sender': self.node_id, 
                'leader': self.node_id, 'clock': dict(self.vector_clock)
            })

    def heartbeat_loop(self):
        while self.running:
            time.sleep(5)
            for pid in self.peers:
                self.async_send(pid, {'type': 'HEARTBEAT', 'sender': self.node_id})

    def command_loop(self):
        self.log("\n" + "="*50)
        self.log(f"Peer {self.node_id} Commands: chat, clock, election, leader, status, quit")
        self.log("="*50)
        while self.running:
            try:
                cmd = input(f"\nPeer {self.node_id}> ").strip()
                if not cmd: continue
                parts = cmd.split(' ', 1)
                action = parts[0].lower()
                
                if action == 'chat' and len(parts) > 1:
                    with self.lock:
                        self.vector_clock[self.node_id] += 1
                        clock_snapshot = dict(self.vector_clock)
                    msg = {'type': 'CHAT', 'sender': self.node_id, 'content': parts[1], 'clock': clock_snapshot}
                    self.log(f"[Peer {self.node_id}] Sending CHAT: {parts[1]} | Clock: {clock_snapshot}")
                    for pid in self.peers: 
                        self.async_send(pid, msg)
                
                elif action == 'clock': self.log(f"Vector Clock: {self.vector_clock}")
                elif action == 'election': threading.Thread(target=self.start_election, daemon=True).start()
                elif action == 'leader': self.log(f"Current Leader: Peer {self.leader_id}")
                elif action == 'status': 
                    self.log(f"Node: {self.node_id} | Port: {self.port} | Leader: {self.leader_id} | Clock: {self.vector_clock}")
                elif action == 'quit':
                    self.running = False
                    break
                else: self.log("Unknown command. Available: chat, clock, election, leader, status, quit")
            except (EOFError, KeyboardInterrupt):
                break

if __name__ == '__main__':
    sys.stdout.reconfigure(line_buffering=True)
    
    if len(sys.argv) < 3:
        print("Usage: python peer.py <node_id> <port> [peer_id:host:port ...]")
        sys.exit(1)
    node_id = int(sys.argv[1])
    port = int(sys.argv[2])
    peers = {}
    for arg in sys.argv[3:]:
        parts = arg.split(':')
        if len(parts) == 3:
            peers[int(parts[0])] = (parts[1], int(parts[2]))
    Peer(node_id, port, peers).start()