import argparse
import socket
import time
import sys
import datetime
import pickle

parser = argparse.ArgumentParser()
parser.add_argument("--from-beginning",type = str)
args = parser.parse_args()


# log = open("log.txt", "a")
zook_port = 2181
broker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# zookeeper = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
# zookeeper.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

# zookeeper.connect(('127.0.0.1', zook_port))
# leader = zookeeper.recv(1024).decode('utf-8')
# leader = str(leader)
# print(int(leader.split(":")[1]))
# if leader.split(':')[0] == "leader":
#     leader = int(leader.split(":")[1])
#     broker.connect(('127.0.0.1', leader))
#     zookeeper.send(bytes("recv",'utf-8'))
broker.connect(('127.0.0.1', 9092))
print("Broker is connected")
broker.send(bytes("testing consumer"," utf-8"))

def new_broker(new_leader):
    # broker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    leader = new_leader
    broker.shutdown()
    broker.connect(('127.0.0.1',leader))
    return

def fetch_message(topic,prev_mesg):
    broker.send(bytes("c_message:"+topic, 'utf-8'))
    mesg = str(broker.recv(1024).decode('utf-8'))
    if prev_mesg != mesg:
        print(mesg,end="")
        prev_mesg = mesg
        # log.write("Last message was fetched from topic {} at {}\n".format(topic,datetime.datetime.now()))
        broker.send(bytes("log;Last message was fetched from topic {} at {}".format(topic,datetime.datetime.now()),'utf-8'))
    return prev_mesg

def fetch_allmessages(topic):
    broker.send(bytes("c_allmessages:"+topic,'utf-8'))
    time.sleep(2)
    mesg = broker.recv(200000)
    data = pickle.loads(mesg)
    # print(str(data),end="")
    for mesg in data:
        print(mesg,end="")
    broker.send(bytes("log;All messages were fetched from topic {} at {}".format(topic,datetime.datetime.now()),'utf-8'))
    # log.write("All messages were fetched from topic {} at {}\n".format(topic,datetime.datetime.now()))
    return data[-1]

def main():
    try:
        prev_mesg = ""
        print("Enter topic name")
        topic = input()
        broker.send(bytes("c_topic:"+topic,'utf-8'))
        time.sleep(2)
        mesg = str(broker.recv(1024).decode('utf-8'))
        if mesg.split(':')[0] == "Yes":
            print(mesg.split(':')[1])
            if args.from_beginning:
                prev_mesg = fetch_allmessages(topic)
            # print("----------")
            while True:
                # print("loop has started")
                # zook_mesg = str(zookeeper.recv(1024).decode('utf-8'))
                # if zook_mesg.split(':')[0] == "leader" and zook_mesg.split(':')[1] != leader:
                #     new_broker(zook_mesg.split(":")[1])
                # time.sleep(2)
                # prev_mesg = fetch_message(topic,prev_mesg)
                prev_mesg = fetch_message(topic,prev_mesg)
        else:
            print("No topic found")
            # zookeeper.send(bytes("terminate", 'utf-8'))
            # zookeeper.shutdown()
            # zookeeper.close()
            broker.shutdown(socket.SHUT_RDWR)
            broker.send(bytes("terminate", 'utf-8'))
            broker.close()
            sys.exit()         
    except KeyboardInterrupt:
        # zookeeper.send(bytes("terminate", 'utf-8'))
        # zookeeper.shutdown()
        # zookeeper.close()
        broker.send(bytes("terminate", 'utf-8'))
        broker.shutdown(socket.SHUT_RDWR)
        broker.close()
        sys.exit()

if __name__ == "__main__":
    main()
