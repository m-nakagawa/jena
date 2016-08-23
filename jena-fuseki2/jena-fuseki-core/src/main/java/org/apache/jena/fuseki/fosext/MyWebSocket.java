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

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;

import org.apache.jena.fosext.RealtimeValueBroker;
import org.eclipse.jetty.websocket.api.RemoteEndpoint;
import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeRequest;
import org.eclipse.jetty.websocket.servlet.ServletUpgradeResponse;
import org.eclipse.jetty.websocket.servlet.WebSocketCreator;
import org.slf4j.Logger;

// MNakagawa
@WebSocket
public class MyWebSocket implements RealtimeValueBroker.ValueConsumer {
    private final static Logger log = getLogger(MyWebSocket.class);

	private Session session;
	private boolean alive;
	private final static Pattern PATH_PATTERN = Pattern.compile("/");
	
	public static class MyWebSocketCreator implements WebSocketCreator
	{
	    @Override
	    public Object createWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp)
	    {
	    	try {
	    		return new MyWebSocket(req, resp);
	    	}
	    	catch(DataFormatException e){
	    		log.error(e.toString(),e);
	    		return null;
	    	}
	    }
	}

	private List<RealtimeValueBroker.RootProxy> targets;
	private List<RealtimeValueBroker.RootProxy> findProxies(String path) throws DataFormatException {
		try {
			String epath = URLDecoder.decode(path, "UTF-8");
			
			// fos/<dataset>/(update|read)/(path|id|query)/....
			String[] apath = PATH_PATTERN.split(epath);
			if(apath.length < 5 ){
				throw new DataFormatException("Illegal path format:"+epath);
			}
			String dataset = apath[2];
			String op = apath[3];
			String searchBy = apath[4];
			switch(searchBy){
			case "id":
				if(apath.length != 6){
					throw new DataFormatException("Illegal path format:"+epath);
				}
				String id = RealtimeValueBroker.FOS_PROXY_HOLDER+apath[5];
				RealtimeValueBroker.RootProxy[] proxy = new RealtimeValueBroker.RootProxy[1]; 
				proxy[0] = RealtimeValueBroker.getRootProxy(id);
				if(proxy[0] == null){
					throw new DataFormatException("Undefined id:"+epath);
				}
				return Arrays.asList(proxy);
				
			default:
				throw new DataFormatException("Unknown operation:"+searchBy+":"+epath);
			}
		}
		catch (UnsupportedEncodingException e ){
			throw new DataFormatException("Illegal path format:"+path);
		}
	}
	
	
	public MyWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) throws DataFormatException {
		this.targets = findProxies(req.getRequestPath());
		this.alive = true;
		this.session = null;
		for(RealtimeValueBroker.RootProxy proxy : this.targets){
			proxy.addConsumer(this);
		}
	}

	
	private static final String ValueTag = "http://bizar.aitc.jp/ns/fos/0.1/値";
	
	@Override
	public boolean informValueUpdate(RealtimeValueBroker.RootProxy proxy){
		if(this.alive && this.session != null){
			Map<String,RealtimeValueBroker.LeafProxy> leaves = proxy.getLeaves();
			RealtimeValueBroker.LeafProxy leaf = leaves.get(ValueTag);
			if(leaf != null){
				String msg = String.format("%s\t%s\t%s",
						proxy.getURI(),
						ValueTag,
						leaf.getCurrentValue().getLiteral().getLexicalForm());
				this.session.getRemote().sendStringByFuture(msg);
			}
			return true;
		}
		else {
			return false;
		}
	}
	
	@OnWebSocketConnect
	public void onConnect(Session session) {
		this.session = session;
		System.out.println("Connect");
		Future<Void> fut = session.getRemote().sendStringByFuture("HELLO");
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
		System.out.println("onMessage: " + message);
		Future<Void> fut = session.getRemote().sendStringByFuture("ANS:"+message);
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

