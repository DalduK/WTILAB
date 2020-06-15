import json
import time

import requests

def test():
    url = 'http://127.0.0.1:8080/'
    myobj = {"userID": 75, "movieID": 44000, "rating": 4.0, "genre-Action": 0, "genre-Adventure": 0, "genre-Animation": 0, "genre-Children": 0, "genre-Comedy": 0, "genre-Crime": 0, "genre-Documentary": 0, "genre-Drama": 1,"genre-Fantasy": 0, "genre-Film-Noir": 0, "genre-Horror": 0, "genre-IMAX": 0, "genre-Musical": 0,"genre-Mystery": 1, "genre-Romance": 1, "genre-Sci-Fi": 0, "genre-Short": 0, "genre-Thriller": 1, "genre-War":0, "genre-Western": 0}
    reqlist = []

    y1 = requests.get(url + 'ratings')
    reqlist.append(y1)
    y2 = requests.get(url + 'avg-genre-ratings/all-users')
    reqlist.append(y2)
    y3 = requests.get(url + 'avg-genre-ratings/78')
    reqlist.append(y3)
    y3 = requests.get(url + 'profile/75')
    reqlist.append(y3)
    for i in reqlist:
        time.sleep(0.01)
        print("request url: " +  str(i.url))
        print("request status_code: " + str(i.status_code))
        print("request headers: " + str(i.headers))
        print("request text: " + str(i.text))
        print("request request.headers: " + str(i.request.headers))
        print('-------------------------------------------------')
        print('-------------------------------------------------')

if __name__ == '__main__':
    test()