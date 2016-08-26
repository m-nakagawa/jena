path1 ='ws://localhost:3030/fos/test02/read/path/温度?test=123';
path2 ='ws://localhost:3030/fos/test02/update/path/2F/居間/温度';
    //var ws = new WebSocket(path1);
    //var ws = new WebSocket('ws://localhost:3030/fos/update/id/1');
cnt = 1
ws = 0
connect1 = function (){
    ws = new WebSocket(path1);
    ws.onmessage = function (me) {
	var receivedData = me.data;
	console.log("Message is exists!!");
	console.log("recievedData: " + receivedData);
	try {
	    var j = $.parseJSON(receivedData);
	    var tgt = "div#w"+j.id;
	    console.log(tgt);
	    $(tgt).text(receivedData);
	    if(j.id == 9 && ws2){
		ws2.send('{"値":'+cnt+'}');
	    }
	    cnt = cnt+1;
	}catch(e){
	    console.log(e)
	    //alert(e);
	}
    };

    ws.onclose = function (me) {
	console.log("Colosed")
	connect1()
    }
}
    console.log("Start");
    connect1()
    ws.onopen = function () {
	console.log("Connecting is success!!");
    };



var ws2 = new WebSocket(path2)

    ws2.onopen = function () {
    console.log("2 Connecting is success!!");
    ws.send('{"値":"abc"}}');
};

ws2.onmessage = function (me) {
    var receivedData = me.data;
    console.log("Message is exists!!");
    console.log("recievedData: " + receivedData);
    //var j = $.parseJSON(receivedData);
    // console.log("id:"+j.id);
    //$("div#w2").text(receivedData);
};

ws2.onclose = function (me) {
    console.log("2Colosed")
}
