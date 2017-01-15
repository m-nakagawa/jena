#-*- coding: utf-8 -*-
'''
@author: m-nakagawa
'''
import urllib
from tornado import ioloop
from ws4py.client.threadedclient import WebSocketClient
import json

is_closing = False

_W = None
_Evlist = {}
_Positive = -1

def send_positive(cnt):
    global _W
    values = {u"総計":cnt }
    #sendvalue = [['vote-10', values]]
    sendvalue = [['cnt-0', values]]
    print json.dumps(sendvalue,ensure_ascii=False)
    _W.send(json.dumps(sendvalue,ensure_ascii=False))


def cal_positive():
    global _Positive
    cnt = 0
    for o in _Evlist.keys():
        if _Evlist[o] > 50:
            cnt += 1
    #print cnt
    if _Positive != cnt:
        _Positive = cnt
        print "Positive:",cnt
        send_positive(cnt)
    pass

class ReceiveClient(WebSocketClient):
    def opened(self):
        print "opened:", self.num
        pass

    def received_message(self, m):
        #print "Message", m.data.decode('utf-8')
        j = json.loads(m.data)
        o = j[0]
        if o == "cnt-0":
            print "IGNORE"
            return
        if o == "_system":
            print "IGNORE"
            return
        point = j[1][u'評価']
        if point != "":
            _Evlist[o] = int(point)
        else:
            print "xxxx ", o
        cal_positive()

        #print json.dumps(j)
        #print json.dumps(j,ensure_ascii=False)
        pass

    def closed(self, code, reason=None):
        global _W
        print "Closed", self.num
        _W = None
        pass
    
    def set_num(self,num):
        self.num = num

_Base = u"ws://localhost:3030/openmasami/"
_Query1 = u"sample01/read/path/device/value"
_Parm=[
    ('-latest',"1"),
    ]
_Url = _Base+urllib.quote(_Query1.encode('utf-8'))+"?"+urllib.urlencode(_Parm)

if __name__ == '__main__':
    print(_Url)

    try:
        global _W
        _W = ReceiveClient( _Url,
                             protocols=['http-only', 'chat'])
        if _W is None:
            exit(1)
        _W.set_num(1)
        _W.connect()
        _W.run_forever()
    except KeyboardInterrupt:
        _W.close()

