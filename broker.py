# commandline arg:ip and port
import sys
import socket
import os
import threading
import datetime
import time
import pickle

port = int(sys.argv[1])
ip = sys.argv[2]

broker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
broker.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
broker.bind((ip,port))
broker.listen(10)

global_lock = threading.Lock()

def write_log(log_entry):
    while global_lock.locked():
        continue
    global_lock.acquire()
    with open("log.txt", "a") as log:
        log.write(log_entry+"\n")
        log.close()
    global_lock.release()

def read_topics():
    topics = os.listdir("topics")
    for i in range(len(topics)):
        topics[i] = str(topics[i]).split(".")[0]
    return topics

def read_allmessages(topic):
    with open("topics/"+topic+".txt", "r") as f:
        messages = f.readlines()
    return messages

def read_lastmessage(topic):
    messages = []
    if "c_message" in topic:
        return
    else:
        with open("topics/"+topic+".txt", "r") as f:
            messages = f.readlines()
            if messages == []:# in case of an empty file
                return ""
            return messages[-1]

def write_message(topic, message):
    with open("topics/"+topic+".txt", "a") as f:
        f.write(message+"\n")
        
        
def process(conn):
    try:
        # count = 0
        # log = open("log.txt", "a")
        while True:
            # print(count)
            topics = read_topics()
            # conn,_ = broker.accept()
            prev_mesg = ""
            mesg = conn.recv(1024).decode('utf-8')
            mesg = str(mesg)
            if mesg.split(":")[0] == "p_topic":
                topic_p = mesg.split(":")[1]
                if topic_p in topics:
                    pass
                else:
                    with open("topics/"+topic_p+".txt", "w") as f:
                        pass
                    topics = read_topics()
                conn.send(bytes("Topic found", 'utf-8'))
                time.sleep(1)
                    
            elif mesg.split(":")[0] == "p_message":
                topic = mesg.split(":")[1]
                message = mesg.split(":")[2]
                write_message(topic, message)
                # conn.send(bytes("Message sent",'utf-8'))
                write_log("Message written to topic {} at {}".format(topic, datetime.datetime.now()))
                time.sleep(1)
            
            elif mesg.split(":")[0] == "c_topic":
                topic_c = mesg.split(":")[1]
                topics = read_topics()
                if topic_c in topics:
                    # print("found")
                    conn.send(bytes("Yes:Fetching Messages..", 'utf-8'))
                else:
                    conn.send(bytes("No:", 'utf-8'))
                time.sleep(1)
                    
            elif mesg.split(":")[0] == "c_allmessages":
                messages = read_allmessages(mesg.split(":")[1])
                # for message in messages:
                #     conn.send(bytes(message, 'utf-8'))
                #     time.sleep(0.5)
                data = pickle.dumps(messages)
                conn.send(bytes(data))
                # write_log("All messages were fetched from topic {} at {}\n".format(topic_c,datetime.datetime.now()))
            
            elif mesg.split(":")[0] == "c_message":
                message = str(read_lastmessage(mesg.split(":")[1]))
                conn.send(bytes(message, 'utf-8'))
                # time.sleep(2)
                # write_log("Last message was fetched from topic {} at {}\n".format(topic_c,datetime.datetime.now()))
            elif mesg == "terminate":
                conn.close()
                break
            elif mesg.split(";")[0] == "log":
                write_log(mesg.split(";")[1])
            # else:
            #     print(mesg)
            # count = count + 1
            # if count>10:
            #     break
    except KeyboardInterrupt:
        # log.close()
        sys.exit()

def main():
    # broker = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    # broker.bind((ip,port))
    # broker.listen(10)
    print("Broker is listening")
    print(read_topics())
    while True:
        conn,_ = broker.accept()
        threading.Thread(target=process,args = (conn,)).start()

if __name__ == "__main__":
    main()