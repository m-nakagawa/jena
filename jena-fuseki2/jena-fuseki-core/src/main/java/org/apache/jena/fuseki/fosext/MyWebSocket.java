/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.jena.fuseki.fosext;

import static org.slf4j.LoggerFactory.getLogger;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.DataFormatException;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.json.JsonValue;
import org.apache.jena.fosext.FosNames;
import org.apache.jena.fosext.RealtimeValueBroker;
import org.apache.jena.fosext.TimeSeries;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketClose;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.slf4j.Logger;

// MNakagawa
/**
 * @author m-nakagawa
 *
 */
@WebSocket
public class MyWebSocket implements RealtimeValueBroker.ValueConsumer {
    private final static Logger log = getLogger(MyWebSocket.class);

	private Session session;
	private boolean alive;
	private RealtimeValueUtil.TargetOperation targetOperation;
	
	
	public static class MyWebSocketCreator implements WebSocketCreator
	{
	    @Override
	    public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp)
	    {
	    	try {
	    		return new MyWebSocket(req, resp);
	    	}
	    	catch(DataFormatException e){
	    		//log.error(e.toString(),e);
	    		log.error(e.toString());
	    		return null;
	    	}
	    }
	}

	
	public MyWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) throws DataFormatException {
		this.alive = true;
		this.session = null;
		String path = req.getRequestPath();
		
		final Map<String,List<String>> parms = req.getParameterMap();
		this.targetOperation = RealtimeValueUtil.findTargets(path, (index)->parms.get(index)); 

		if(this.targetOperation.getTargets() != null){
			for(RealtimeValueBroker.HubProxy proxy : this.targetOperation.getTargetArray()){
				//TODO こたえにnullを含まないようにする
				if(proxy != null){
					proxy.addConsumer(this);
				}
			}
		}
	}

	/*
	private void informAll(){
		for(RealtimeValueBroker.HubProxy p: this.targetOperation.getTargetArray()){
			//TODO proxy != nullであるようにする
			if(p != null){
				this.informValueUpdate(p);
			}
		}
	}
	*/
	
	private void informHistory(RealtimeValueBroker.HubProxy p, int limit){
		TimeSeries ts = p.getTimeSeries();
		final RemoteEndpoint peer = this.session.getRemote();
		final String pre = "[\""+p.getIdStr()+"\",";
		final String post = "]";
		ts.getHistory(limit).forEach(h->{
			peer.sendStringByFuture(pre+h+post);
		});
	}
	
	private void informHistory(int limit){
		for(RealtimeValueBroker.HubProxy p: this.targetOperation.getTargetArray()){
			informHistory(p, limit);
		}
	}
	
	@Override
	public boolean informValueUpdate(RealtimeValueBroker.HubProxy proxy){
		if(this.alive && this.session != null){
			//System.err.println("Inform:"+proxy.getURI());
			this.session.getRemote().sendStringByFuture(proxy.toJSON());
			return true;
		}
		else {
			return false;
		}
	}
	
	@OnWebSocketConnect
	public void onConnect(Session session) {
		this.session = session;
		log.debug("Connect");
		System.err.println("Connect");
		
		if(this.targetOperation.getHistory() >= 0){
			this.informHistory(this.targetOperation.getHistory());
		}
		/*
		else if(this.targetOperation.getOperation() == RealtimeValueUtil.Operation.READ){
			this.informAll();
		}
		*/
		//session.getRemote().sendStringByFuture("HELLO");
		/*
		try {
			session.getRemote().sendString("Hello.");
		}
		catch(IOException ex){
			System.out.println("Connect: IOException"+ex.toString());
		}
		*/
	}


	@OnWebSocketMessage
	public void onText(String message) {
		JsonObject msg = JSON.parse("{ \"value\":"+message+"}");

		//System.err.println("onMessage: " + message);
		JsonValue j = msg.get("value"); 
		if(j.isArray()){
			j.getAsArray().forEach(v->{
				RealtimeValueBroker.HubProxy[] proxy = new RealtimeValueBroker.HubProxy[1];
				JsonArray a = v.getAsArray();
				proxy[0] = this.targetOperation.getTargetById(a.get(0).getAsString().value());
				if(proxy[0] != null){
					setAll(a.get(1), proxy);
				}
			});
		}
		else {
			setAll(j, this.targetOperation.getTargetArray());
		}
	}
	
	private void setAll(JsonValue j, RealtimeValueBroker.HubProxy[] targets){
    	List<RealtimeValueBroker.Pair<String, RealtimeValueBroker.Value[]>> values = new ArrayList<>(); 
		for(Entry<String,JsonValue> e: j.getAsObject().entrySet()){
			String key = FosNames.FOS_NAME_BASE+e.getKey();
			JsonValue v = e.getValue();
			RealtimeValueBroker.Value[] value;
			if(v.isArray()){
				//System.err.println("++++a "+key+"  "+v.toString());
				JsonArray varray = v.getAsArray();
				value = new RealtimeValueBroker.Value[varray.size()];
				for(int i = 0; i < varray.size(); ++i){
					JsonValue vv = varray.get(i);
					if(vv.isString()){
						value[i] = RealtimeValueUtil.str2value(vv.getAsString().value());
					}
					else {
						value[i] = RealtimeValueUtil.str2value(vv.toString());
					}
				}
			}
			else {
				//System.err.println("++++ "+key+"  "+v.toString());
				value = new RealtimeValueBroker.Value[1];
				if(v.isString()){
					value[0] = RealtimeValueUtil.str2value(v.getAsString().value());
				}
				else {
					value[0] = RealtimeValueUtil.str2value(v.toString());
				}
			}
			values.add(new RealtimeValueBroker.Pair<>(key, value));
    	}
    	
    	RealtimeValueBroker.UpdateContext context = null;
    	try {
    		context = RealtimeValueBroker.prepareUpdate();
    		for(RealtimeValueBroker.HubProxy p: targets){
    			//TODO p != nullであるようにする
    			if(p != null){
    				//System.err.println("---"+p.getURI());
    				for(RealtimeValueBroker.Pair<String, RealtimeValueBroker.Value[]> e: values){
    					p.setValues(e.getKey(), e.getValue(), context.getInstant());
    				}
    			}
    		}
    	}
    	finally {
    		if(context != null){
    	    	RealtimeValueBroker.finishUpdate(context);
    		}
    	}
    	
    	//session.getRemote().sendStringByFuture("ANS:"+message);
		// エコーする
		/*
		try {
			this. session.getRemote().sendString("Ans: "+message);
		}
		catch(IOException ex){
			System.out.println("Connect: IOException"+ex.toString());
		}
		*/
	}

	@OnWebSocketClose
	public void onClose(Session session, int statusCode, String reason){
		log.debug("Close");
		System.err.println("Close");
		this.session = null;
		this.alive = false;
	}
	/*
	private Session session;
	
	@OnWebSocketConnect
	public void onConnect(Session session) {
		this.session = session;
		System.out.println("Connect");
		try {
			session.getBasicRemote().sendText("Hello.");
		}
		catch(IOException ex){
			System.out.println("Connect: IOException"+ex.toString());
		}
	}

	@OnWebSocketMessage
	public void onText(String message) {
		System.out.println("onMessage: " + message);
		// エコーする
		try {
			this.session.getBasicRemote().sendText("Ans: "+message);
		}
		catch(IOException ex){
			System.out.println("Connect: IOException"+ex.toString());
		}
	}

	@OnWebSocketClose
	public void onClose(int statusCode, String reason) {
		System.out.println("Close:"+reason);
	}
	*/
}

