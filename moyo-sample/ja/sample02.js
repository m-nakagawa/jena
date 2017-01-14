//日本語

cnt = 0;
connect1 = function (path){
    ws = new WebSocket(path);
    ws.path = path
    ws.onerror = function (me) {
	console.log("ERROR");
	ws.errorflag = true;
    }
    ws.onopen = function () {
	console.log("Connected");
    };

    ws.onmessage = function (me) {
	var receivedData = me.data;
	console.log("recievedData: " + receivedData);
	try {
	    var j = $.parseJSON(receivedData);
	    var tgt = "div#"+j[0];
	    console.log(tgt);
	    $(tgt).text(receivedData);
	    if(cnt== 0){
		// 送信のサンプル
		++cnt;
	        ws.send('[["facesensor-2",{"検出":["次郎","ポチ"]}]]');
	    }
	}catch(e){
	    console.log(e)
	    //alert(e);
	}
    };
    ws.onclose = function (me) {
	console.log("Colosed")
	if(!ws.errorflag){
	    connect1(ws.path)
	}
    }
}
