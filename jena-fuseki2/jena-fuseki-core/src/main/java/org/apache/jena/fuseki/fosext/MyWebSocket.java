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

import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.DataFormatException;

import org.apache.jena.atlas.json.JSON;
import org.apache.jena.atlas.json.JsonArray;
import org.apache.jena.atlas.json.JsonObject;
import org.apache.jena.atlas.json.JsonValue;
import org.apache.jena.fosext.CountLogger;
import org.apache.jena.fosext.HubProxy;
import org.apache.jena.fosext.LeafValue;
import org.apache.jena.fosext.RealtimeValueBroker;
import org.apache.jena.fosext.TimeSeries;
import org.apache.jena.fosext.ValueConsumer;
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

/**
 * @author m-nakagawa
 *
 */
@WebSocket
public class MyWebSocket implements ValueConsumer {
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
	    		log.error(e.toString(),e);
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
			for(HubProxy proxy : this.targetOperation.getTargetArray()){
				//TODO こたえにnullを含まないようにする
				if(proxy != null){
					proxy.addConsumer(this);
				}
			}
		}
	}

	private void informAll(){
		for(HubProxy p: this.targetOperation.getTargetArray()){
			//TODO proxy != nullであるようにする
			if(p != null){
				this.informValueUpdate(p);
			}
		}
	}
	
	private void informHistory(HubProxy p, int limit){
		TimeSeries ts = p.getTimeSeries();
		final RemoteEndpoint peer = this.session.getRemote();
		final String pre = "[\""+p.getIdStr()+"\",";
		final String post = "]";
		ts.getHistory(limit).forEach(h->{
			String text = pre+h+post;
			peer.sendStringByFuture(text);
			log.trace("Socket History:"+text);
		});
		peer.sendStringByFuture("");
	}
	
	private void informHistory(int limit){
		for(HubProxy p: this.targetOperation.getTargetArray()){
			informHistory(p, limit);
		}
	}
	
	@Override
	public boolean informValueUpdate(HubProxy proxy){
		if(this.alive && this.session != null){
			this.session.getRemote().sendStringByFuture(proxy.toJSON());
			log.trace("Socket send:"+proxy.toJSON());
			RealtimeValueBroker.getSendCnt().inc();
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
		
		CountLogger wscnt = RealtimeValueBroker.getWebsocketCnt();
		wscnt.inc();
		if(this.targetOperation.getHistory() >= 0){
			this.informHistory(this.targetOperation.getHistory());
		}
		if(this.targetOperation.getLatest()){
			this.informAll();
		}
	}


	@OnWebSocketMessage
	public void onText(String message) {
		log.trace("Socket receive:"+message);
		//いきなり配列だとパーズできないので、ハッシュ化
		JsonObject msg = JSON.parse("{ \"value\":"+message+"}");

		JsonValue j = msg.get("value"); 
		if(j.isArray()){
			// [[node,{values}],[node,{values}]...]
			j.getAsArray().forEach(v->{
				HubProxy[] proxy = new HubProxy[1];
				JsonArray a = v.getAsArray();
				proxy[0] = this.targetOperation.getTargetById(a.get(0).getAsString().value());
				if(proxy[0] != null){
					setAll(a.get(1), proxy);
				}
			});
		}
		else {
			// {values}
			setAll(j, this.targetOperation.getTargetArray());
		}
	}
	
	private void setAll(JsonValue j, HubProxy[] targets){
    	List<Entry<String, LeafValue[]>> values = new ArrayList<>(); 
		for(Entry<String,JsonValue> e: j.getAsObject().entrySet()){
			//String key = FosNames.FOS_NAME_BASE+e.getKey();
			String key = this.targetOperation.getPropertyNamespace()+e.getKey();
			JsonValue v = e.getValue();
			LeafValue[] value;
			if(v.isArray()){
				JsonArray varray = v.getAsArray();
				value = new LeafValue[varray.size()];
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
				value = new LeafValue[1];
				if(v.isString()){
					value[0] = RealtimeValueUtil.str2value(v.getAsString().value());
				}
				else {
					value[0] = RealtimeValueUtil.str2value(v.toString());
				}
			}
			values.add(new AbstractMap.SimpleEntry<>(key, value));
    	}
    	
    	RealtimeValueBroker.UpdateContext context = null;
    	try {
    		context = RealtimeValueBroker.prepareUpdate(false);
    		for(HubProxy p: targets){
    			//TODO p != nullであるようにする
    			if(p != null){
    				for(Entry<String, LeafValue[]> e: values){
    					p.setValues(e.getKey(), e.getValue(), context);
    				}
    			}
    		}
    	}
    	finally {
    		if(context != null){
    	    	RealtimeValueBroker.finishUpdate(context);
    		}
    	}
	}

	@OnWebSocketClose
	public void onClose(Session session, int statusCode, String reason){
		log.debug("Close");
		this.session = null;
		this.alive = false;

		CountLogger wscnt = RealtimeValueBroker.getWebsocketCnt();
		wscnt.dec();
	}
}

