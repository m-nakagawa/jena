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
import java.util.List;
import java.util.Map;
import java.util.concurrent.Future;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;

import org.apache.jena.fosext.RealtimeValueBroker;
import org.apache.jena.graph.Node;
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

	private final static Pattern PATH_PATTERN = Pattern.compile("/");

	private Session session;
	private boolean alive;
	private Operation operation;
	private RealtimeValueBroker.HubProxy[] targets;
	
	
	private static enum Operation {
		UPDATE,
		READ,
		;
	}
	
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

	private static final String HUB = "hub";
	private static final String QUERY_HEAD =
			"PREFIX :        <"+RealtimeValueBroker.FOS_NAME_BASE+">\n"+ 
			"SELECT ?hub\n"+
			"WHERE {\n";

	private static final String QUERY_MIDDLE1 =
			"      ?s%d :パス識別子 <%s> .\n";
	
	private static final String QUERY_MIDDLE2 =
			"      ?s%d ?p%d ?s%d .\n" ;

	private static final String QUERY_TAIL =
			"      ?s%d <%s> ?"+HUB+" .\n"+
			"}\n";

	private static final String DEFAULT_PROXY_NAMESPACE = RealtimeValueBroker.FOS_TAG_BASE;
	private static final String DEFAULT_PROPERTY_NAMESPACE = RealtimeValueBroker.FOS_NAME_BASE;
		
	/**
	 * タグパスにマッチする物理ノードを検索する
	 *  <pre>
	 *   /fos/<update|read>/path/<dataset>/<tag0>/<tag1>/.../<tagN>
	 *   pParts[offset] : <dataset>
	 *  </pre>
	 * @param pParts
	 * @param offset
	 * @return
	 * @throws DataFormatException
	 */
	public static RealtimeValueBroker.HubProxy[] findProxiesByPath(String[] pParts, int offset) throws DataFormatException {
		String partsNamespace = DEFAULT_PROXY_NAMESPACE;
		String propertyNamespace = DEFAULT_PROPERTY_NAMESPACE;
		if(offset+2 > pParts.length){
			throw new DataFormatException("No path specified");
		}
		
		String datasetName = pParts[offset];
		
		// SPARQLクエリを生成する
		StringBuilder queryString = new StringBuilder(QUERY_HEAD);
		int i = offset+1;
		if(i < pParts.length-1){
			for(;; ++i){
				queryString.append(String.format(QUERY_MIDDLE1, i, partsNamespace+pParts[i]));
				if(i>=pParts.length-2){
					break;
				}
				queryString.append(String.format(QUERY_MIDDLE2, i, i, i+1));
			}
			queryString.append(String.format(QUERY_TAIL, i, propertyNamespace+pParts[i+1]));
		}
		else {
			queryString.append(String.format(QUERY_TAIL, 0, propertyNamespace+pParts[i]));
		}
		System.err.println(queryString.toString());
		
		// クエリを実行する
		List<Map<String,String>> result = SparqlAccess.execute(datasetName, queryString.toString());
		if(result.size() == 0){
			throw new DataFormatException("No path found");
		}
		
		// 結果を抽出する
		RealtimeValueBroker.HubProxy[] ret = new RealtimeValueBroker.HubProxy[result.size()];
		for(int j =0; j<result.size();++j){
			String id = result.get(j).get("hub");
			ret[j] = RealtimeValueBroker.getRootProxy(id);
			if(ret[j] == null){
				throw new DataFormatException(String.format("Unknown id:%s", id));
			}
		}
		return ret;
	}
	
	public static RealtimeValueBroker.HubProxy findProxyById(String label) throws DataFormatException {
		String id = RealtimeValueBroker.FOS_PROXY_HOLDER+label;
		RealtimeValueBroker.HubProxy proxy = RealtimeValueBroker.getRootProxy(id);
		if(proxy == null){
			throw new DataFormatException(String.format("Unknown id:%s", id));
		}
		return proxy;
	}
	
	private RealtimeValueBroker.HubProxy[] findProxies(Operation operation, String[] pParts, int offset) throws DataFormatException {
			String selector = pParts[offset];
			switch(selector){
			case "path":
				return findProxiesByPath(pParts, offset+1);

			case "id":
				if(pParts.length-offset != 2){
					throw new DataFormatException("Illegal path format");
				}
				RealtimeValueBroker.HubProxy[] proxy = new RealtimeValueBroker.HubProxy[1]; 
				proxy[0] = findProxyById(pParts[offset+1]); 
				return proxy;
				
			default:
				throw new DataFormatException(String.format("Undefined selector:%s", selector));
			}
	}
	
	
	public MyWebSocket(ServletUpgradeRequest req, ServletUpgradeResponse resp) throws DataFormatException {
		Map<String,List<String>> parms = req.getParameterMap();
		this.alive = true;
		this.session = null;

		String path = req.getRequestPath();
		String epath;
		try {
			epath = URLDecoder.decode(path, "UTF-8");
		}
		catch (UnsupportedEncodingException e ){
			throw new DataFormatException("Illegal path format:"+path);
		}
		
		// fos/(update|read)/(path|id|query)/<dataset>/....
		String[] pParts = PATH_PATTERN.split(epath);
		if(pParts.length < 4 ){
			throw new DataFormatException("Illegal path format:"+epath);
		}
		
		try {
			this.operation = Operation.valueOf(pParts[2].toUpperCase());
		}
		catch(IllegalArgumentException e){
			throw new DataFormatException(String.format("Unknown operation:%s:%s", pParts[2], epath));
		}
		try {
			this.targets = findProxies(this.operation, pParts, 3);
		}
		catch(DataFormatException e){
			throw new DataFormatException(String.format("%s:%s", e.toString(), epath));
		}

		for(RealtimeValueBroker.HubProxy proxy : this.targets){
			proxy.addConsumer(this);
		}
	}

	private void informAll(){
		for(RealtimeValueBroker.HubProxy p: this.targets){
			this.informValueUpdate(p);
		}
	}
	
	@Override
	public boolean informValueUpdate(RealtimeValueBroker.HubProxy proxy){
		if(this.alive && this.session != null){
			List<RealtimeValueBroker.KVPair> values = proxy.getPropertyValuePairs();
			StringBuilder ret = new StringBuilder();
			ret.append('{');
			for(int i = 0;;){
				RealtimeValueBroker.KVPair p = values.get(i);
				ret.append('"');
				ret.append(p.getKey());
				ret.append("\":");
				Node n = p.getValue();
				if(n.isLiteral()){
					Object o = n.getLiteralValue();
					if(o instanceof Number){
						ret.append(o.toString());
					}
					else {
						ret.append('"');
						ret.append(o.toString());
						ret.append('"');
					}
				}
				else {
					ret.append('"');
					ret.append(n.toString());
					ret.append('"');
				}
				//ret.append(p.getValue());
				if(++i == values.size()){
					break;
				}
				ret.append(',');
			}
			ret.append('}');
			/*
			Map<String,RealtimeValueBroker.LeafProxy> leaves = proxy.getLeaves();
			RealtimeValueBroker.LeafProxy leaf = leaves.get(ValueTag);
			if(leaf != null){
				String msg = String.format("%s\t%s\t%s",
						proxy.getURI(),
						ValueTag,
						leaf.getCurrentValue().getLiteral().getLexicalForm());
				this.session.getRemote().sendStringByFuture(msg);
			}
			*/
			this.session.getRemote().sendStringByFuture(ret.toString());
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
		if(this.operation == Operation.READ){
			this.informAll();
		}
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

	@OnWebSocketClose
	public void onClose(Session session, int statusCode, String reason){
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

