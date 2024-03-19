#!/bin/python3
import os, time
import sys
import requests
import random

if len(sys.argv) < 2:
  print("Usage:./multi-process-curl.py <concurrency>")
  sys.exit(-1)

concurrency = int(sys.argv[1])
print(f"concurrency:{concurrency}")


def do_work(iteration):
  for i in range(iteration):
    page_id = random.randint(0,8)
    resp=requests.get("http://172.31.29.53:28080/v1/file/07a764997fae49f0ffaa2a283d194304c78a61d4a56052abbbdbafd25da81745/page/" + str(page_id))
    len(resp.content)

st = time.time()
i_am_child = False
for i in range(concurrency):
  processid = os.fork()
  if processid <= 0:
    i_am_child = True
    print(f"Child Process:{i}")
    #print("start sleeping for 3 sec...")
    #time.sleep(3)
    do_work(1000)
    print(f"Child Process:{i} exit")
    break
  else:
    print(f"Parent Process, {i}th Child process, id:{processid}")
if not i_am_child:
  os.wait()
  ed = time.time()
  print(f"total time:{ed-st}")

