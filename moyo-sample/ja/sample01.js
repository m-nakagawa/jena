// -*- coding: utf-8 -*-

id = Math.floor( Math.random() * 10000 ) ;
vote_name = "名前"+id

h= Math.floor(Math.random() * 360) ;
vote_color = "hsl("+h+",80%,50%)";
prevChangeValue = 0;

window.onload = function() {  
    vote_name_input = document.getElementById("vote-name");
    vote_name_input.value = vote_name;
    vote_name_input.style.backgroundColor = vote_color;
}


function changeValue2(id, v){
    if(id == 'vote-name'){
	vote_name = v;
	console.log("xxxx:"+v);
	return;
    }
    //console.log(id+":"+v);
    var s =[];
    s[0] = [];
    s[0][0] = id;
    s[0][1] = {};
    s[0][1].評価 = parseInt(v);
    s[0][1].ニックネーム =vote_name;
    s[0][1].色 =vote_color;
    ws.send(JSON.stringify(s));
    //ws.send('[["'+id+'",{"評価":'+v+',"ニックネーム":"'+vote_name+'"}]]');
    prevChangeValue = (new Date).getTime();
}

function changeValue(id, v){
    changeValue2(id,v)
    /*
    // データ量調整
    var now = (new Date).getTime();
    if(now - prevChangeValue > 50){
	changeValue2(id,v);
    }
    */
}

function draw(id, v, color) {
    //console.log("draw:"+id+":"+v);
    var canvas = document.getElementById("c-"+id);
    if ( ! canvas || ! canvas.getContext ) { return false; }
    var ctx = canvas.getContext('2d');
    ctx.beginPath();
    ctx.fillStyle = 'rgb(255,255,255)';
    ctx.fillRect(2*v, 5, 200, 20);
    //ctx.fillStyle = 'rgb(128,128,128)';
    ctx.fillStyle = color;
    ctx.fillRect(0, 5, 2*v, 20);
}


/*------------------*/
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
	    //console.log(j[0])
	    if(j[0] == "cnt-0"){
		$("div#cnt-0").text(j[1].総計);
		return;
	    }
	    if(j[0] == "_system"){
		$("div#_connect").text(j[1].接続数);
		$("div#_maxSend").text(j[1].最大送信数);
		return;
	    }
	    var v = j[1].評価;
	    var tgt_n = "div#n-"+j[0];
	    //console.log(tgt);
	    id = j[0];
	    color = j[1].色;
	    if(color==""){
		color = "#FFFFFF"
	    }
	    //console.log(v);
	    $(tgt).text(v);
	    //$(tgt_n).css("background-color",color);
	    $(tgt_n).text(j[1].ニックネーム);
	    draw(id,v,color);
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
