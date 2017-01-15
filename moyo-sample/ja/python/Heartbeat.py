#-*- coding: utf-8 -*-
'''
Created on 2015/09/14

@author: m-nakagawa
'''

#http://aitc2.dyndns.org/openmasami/sample02/read/id/vote-0?-history=100

import urllib
import thread
import sys
import time

from tornado import ioloop
#from ws4py.client.tornadoclient import TornadoWebSocketClient
from ws4py.client.threadedclient import WebSocketClient
import json

is_closing = False
# http://www.cardiac.jp/view.php?lang=ja&target=normal_ecg_pattern.xml

_Data= [
    25,
    25,
    13.5,
    13.5,
    2,
    2,
    5,
    5,
    6,
    6,

    7,
    7,
    7.5,
    7.5,
    8,
    8,
    8.1,
    8.1,
    8.5,
    8.5,

    9,
    9,
    10,
    10,
    11,
    11,
    12.5,
    12.5,
    13,
    13,

    12,
    12,
    10,
    10,
    9,
    9,
    8,
    8,
    8,
    8,

    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,
    8,

    8,
    8,
    8,
    8,
    8.5,
    8.5,
    9,
    9,
    8.5,
    8.5,
    
    8,3,
    8,3,
    8,
    8,
    7.8,
    7.8,
    7.5,
    7.5,
    13,
    13,
    ]
_ModeTable = [
    (1.0, 3.5),  # 周期、振幅
    (0.8, 3.5),  # 周期、振幅
    (1.1, 3.0),
    (0.5, 3.6),
    ]





class ReceiveClient(WebSocketClient):
    def __init__(self, mode):
        print "--------------",mode
        self.mode = mode
        self.point = mode%20
        self.proxyId = "vote-"+("%d"%(self.point))
        self.name = u"心電計-"+("%d"%(self.point))
        self.xurl =_Url+self.proxyId
        print(_Url)
        WebSocketClient.__init__(self, self.xurl,
                                 protocols=['http-only', 'chat'])
        
    def opened(self):
        def run(*args):
            cnt = 0
            size = len(_Data)
            m = self.mode%len(_ModeTable)
            #step = _ModeTable[m][0]/(size*1)
            step = 0.01
            dstep = _ModeTable[m][0]
            mag = _ModeTable[m][1]
            next = time.clock()+step
            while True:
                now = time.clock()
                wait = next-now
                if wait > 0:
                    time.sleep(wait)
                next += step
                cnt += dstep
                pos = int(cnt)%size
                self.send_value(_Data[pos]*mag)
                #print "test", cnt
        thread.start_new_thread(run, ())
        pass

    def received_message(self, m):
        #print "Message", m.data.decode('utf-8')
        j = json.loads(m.data)
        if j[1][u"評価"] == 0:
            print time.clock()
        #print json.dumps(j,ensure_ascii=False)
        pass

    def closed(self, code, reason=None):
        print "Closed", self.mode
        pass
    
    def send_value(self,cnt):
        now = int(time.clock()*1000)
        #print "xxxx ", now
        values = {u"評価":cnt,u"測定時刻":now,
                  u"色": "#FF0000", u"ニックネーム": self.name }
        #sendvalue = [['vote-10', values]]
        sendvalue = [[self.proxyId, values]]
        #print json.dumps(sendvalue,ensure_ascii=False)
        self.send(json.dumps(sendvalue,ensure_ascii=False))


_Base = u"ws://localhost:3030/openmasami/"
_Query1 = u"sample02/read/id/"
_Parm=[
    ('-latest',"1"),
    ]
_Url = _Base+urllib.quote(_Query1.encode('utf-8'))
#+"?"+urllib.urlencode(_Parm)

if __name__ == '__main__':
    global _ProxyId

    cnt = int(sys.argv[1])

    for mode in range(0,cnt):
        w = ReceiveClient(mode)
        if w is None:
            exit(1)
        w.connect()

    try:
        w.run_forever()
    except KeyboardInterrupt:
        pass

