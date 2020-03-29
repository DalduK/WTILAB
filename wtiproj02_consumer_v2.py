import json
import que
import time

def consumer(name):
    q = que.Que()
    t_end = time.time() + 10
    while time.time() < t_end:
        s = q.get(name)
        if s is None:
            time.sleep(1.00)
            s = q.get(name)
            if s is None:
                print('komunikacja zakonczona')
                break
            else:
                h = json.loads(s.decode('utf-8'))
                print(h)
        else:
            h = json.loads(s.decode('utf-8'))
            print(h)
        time.sleep(1.00/4)