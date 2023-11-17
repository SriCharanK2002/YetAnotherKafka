import socket
import nmap
import subprocess
import datetime
import sys
import threading

brokers = [9092,9093,9094]
ip = '127.0.0.1'
zook_port = 2181
global_lock = threading.Lock()

def write_log(log_entry):
    while global_lock.locked():
        continue
    global_lock.acquire()
    with open("log.txt", "a") as log:
        log.write(log_entry)
        log.close()
    global_lock.release()

def is_port_open(ip, port):
    nm = nmap.PortScanner()
    nm.scan(ip, str(port))
    state = nm[ip]['tcp'][port]['state']
    return state

def tell_producer_consumer(conn,leader):
    send_data = "leader:"+str(leader)
    conn.send(bytes(send_data, 'utf-8'))
    
def start_one(broker1,ip):
    subprocess.run(["python", "broker.py", str(broker1), ip])
    return

def start_two(broker2,ip):
    subprocess.run(["python", "broker.py", str(broker2), ip])
    return
    
def start_three(broker3,ip):
    subprocess.run(["python", "broker.py", str(broker3), ip])
    return

def process(conn):
    try:
        is_leader = 0
        old_leader = 0
        # log = open("log.txt", "a")
        entry = "Leader is {} at {}\n".format(brokers[is_leader], datetime.datetime.now())
        write_log(entry)
        while True:
            tell_producer_consumer(conn,brokers[is_leader])
            if str(conn.recv(1024).decode('utf-8')).split(":")[0] == "recv":
                print("connection established")
                break
        
        print("Leader is {} at {}\n".format(brokers[is_leader], datetime.datetime.now()))
        while True:
            if is_port_open(ip, brokers[is_leader]) == 'open':
                old_leader = is_leader
                is_leader = (is_leader + 1)%3
                print("Broker {} is leader".format(is_leader+1))
                while True:
                    tell_producer_consumer(conn,brokers[is_leader])
                    if str(conn.recv(1024).decode('utf-8')).split(":")[0] == "recv":
                        print("connection established")
                        break
                
                entry = ("Leader is {} at {}\n".format(brokers[is_leader], datetime.datetime.now()))
                write_log(entry)
                tell_producer_consumer(conn,brokers[is_leader])         
                if old_leader == 0:
                    start_one(brokers[old_leader],ip)
                elif old_leader == 1:
                    start_two(brokers[old_leader],ip)
                else:
                    start_three(brokers[old_leader],ip)
            elif conn.recv(1024).decode('utf-8') == "termintate":
                conn.close()
                break
                
    except KeyboardInterrupt:
        sys.exit()
        
def main():
    
    sock = socket.socket()
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind((ip, zook_port))
    sock.listen(10)
    print("Zookeeper is running")
    while True:
        conn,_ = sock.accept()
        threading.Thread(target=process,args = (conn,)).start()
               
if __name__ == "__main__":
    main()