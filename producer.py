import socket
import time
import sys

zook_port = 2181
broker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
broker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1) 
# zookeeper = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# zookeeper.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# zookeeper.connect(('127.0.0.1', zook_port))
# print("Zookeeper is connected")
# leader = zookeeper.recv(1024).decode('utf-8')
# leader = str(leader)
# print(int(leader.split(":")[1]))
# if leader.split(':')[0] == "leader":
#     leader = int(leader.split(":")[1])
#     broker.connect(('127.0.0.1', leader))
#     zookeeper.send(bytes("recv:", 'utf-8'))
broker.connect(('127.0.0.1', 9092))
print("Broker is connected")
broker.send(bytes("testing producer", 'utf-8'))

def new_broker(new_leader):
    # broker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # broker.bind(('127.0.0.1', new_leader))
    broker.shutdown()
    leader = new_leader
    broker.connect(('127.0.0.1',leader))
    return

def send_message(topic):
    # print("Enter message")
    message = input()
    broker.send(bytes("p_message:"+topic+":"+message, 'utf-8'))
    time.sleep(2)
    # print(str(broker.recv(1024).decode('utf-8')))
    return

def main():
    try:
        print("Enter topic name")
        topic = input()
        broker.send(bytes("p_topic:"+topic, 'utf-8'))
        time.sleep(0.5)
        print(str(broker.recv(1024).decode('utf-8')))
        # time.sleep(10)
        # print("Enter message")
        while True:
            # zook_mesg = str(zookeeper.recv(1024).decode('utf-8'))
            # if zook_mesg.split(':')[0] == "leader" and zook_mesg.split(':')[1] != leader:
            #     new_broker(zook_mesg.split(":")[1])
            print("Enter message")
            send_message(topic)
            time.sleep(2)
    except KeyboardInterrupt:
        # zookeeper.send(bytes("terminate", 'utf-8'))
        # zookeeper.shutdown(socket.SHUT_RDWR)
        # zookeeper.close()
        broker.send(bytes("terminate", 'utf-8'))
        broker.shutdown(socket.SHUT_RDWR)
        broker.close()
        sys.exit()
        
if __name__ == "__main__":
    main()