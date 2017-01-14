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
import static org.apache.jena.riot.WebContent.charsetUTF8 ;
import static org.apache.jena.riot.WebContent.contentTypeTextPlain ;

import java.io.IOException ;
import java.io.PrintWriter;
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.zip.DataFormatException;

import javax.servlet.ServletOutputStream ;
import javax.servlet.http.HttpServletRequest ;
import javax.servlet.http.HttpServletResponse ;

import org.apache.jena.fosext.FosNames;
import org.apache.jena.fosext.HubProxy;
import org.apache.jena.fosext.LeafValue;
import org.apache.jena.fosext.RealtimeValueBroker;
import org.apache.jena.fosext.TimeSeries;
import org.apache.jena.fuseki.Fuseki ;
import org.apache.jena.web.HttpSC ;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.slf4j.Logger;


/**
 * @author m-nakagawa
 *
 */
@SuppressWarnings("serial")
public class MyService extends WebSocketServlet
{
    private final static Logger log = getLogger(MyService.class);

    @Override
	public void configure(WebSocketServletFactory factory) {
		factory.getPolicy().setIdleTimeout(0);
		//factory.getPolicy().setIdleTimeout(10000); // 10sec
		//factory.register(MyWebSocket.class);
        // set a custom WebSocket creator
        factory.setCreator(new MyWebSocket.MyWebSocketCreator());
	}

	// Ping is special.
    // To avoid excessive logging and id allocation for a "noise" operation,
    // this is a raw servlet.
    public MyService() {
    	super() ;
    	MyQueryEngine.register();
        Fuseki.serverLog.info("myservice :: MyQueryEngine added.");
    } 
    
    private void sendError(HttpServletResponse resp, int errcode, String text){
    	try{
    		resp.setStatus(errcode);
    		resp.setContentType(contentTypeTextPlain);
    		resp.setCharacterEncoding(charsetUTF8) ;
    		resp.setStatus(HttpSC.OK_200);
    		ServletOutputStream out = resp.getOutputStream() ;
    		out.println(text);
    	} catch (IOException ex) {
    		Fuseki.serverLog.warn("myservice :: IOException :: "+ex.getMessage());
    	}
    }

    private void readOp(HttpServletResponse resp, RealtimeValueUtil.TargetOperation targetOperation){
    	HubProxy[] targets = targetOperation.getTargetArray();
    	int history = targetOperation.getHistory();
    	try{
    		PrintWriter writer = resp.getWriter();
    		writer.println("[");
    		boolean prev = false;
    		for(HubProxy p: targets){
    			//TODO pがnullを含まないように
    			if(p != null){
    				if(prev){
    					writer.println(",");
    				}
    				if(history < 0){
    					writer.print(p.toString());
    				}
    				else {
    					writer.print("["+p.getIdInJSON()+",");
    					writer.println("[");
    					TimeSeries ts = p.getTimeSeries();
    					Iterator<String> i = ts.getHistory(history).iterator();
    					if(i.hasNext()){
    						for(;;){
    							writer.print(i.next());
    							if(i.hasNext()){
    								writer.print(','); //引数が文字型のとき、lnが効かないバグ？？？
    								writer.println();
    							}
    							else {
    								writer.println();
    								break;
    							}
    						}
    					}
    					writer.print("]]");
    				}
    				prev = true;
    			}
    		}
    		writer.println("\n]");
    	} catch (IOException ex) {
    		Fuseki.serverLog.warn("myservice :: IOException :: "+ex.getMessage());
    	}
    }
    

    
    private void writeOp(HttpServletResponse resp, RealtimeValueUtil.TargetOperation targetOperation, Map<String,String[]> parms){
    	HubProxy[] targets = targetOperation.getTargetArray();
    	List<Entry<String, LeafValue[]>> values = new ArrayList<>(); 
    	for(Entry<String,String[]> e: parms.entrySet()){
    		String key = e.getKey();
    		key = FosNames.FOS_TAG_BASE+key;

    		if(RealtimeValueUtil.isEscapeParm(key)){
    			//制御パラメータ
    			continue;
    		}
    		String[] strs = e.getValue();
    		LeafValue[] v = new LeafValue[strs.length]; 
    		for(int i = 0; i < strs.length; ++i){
    			log.trace(key+"  "+strs[i]);
        		v[i] = RealtimeValueUtil.str2value(strs[i]);
    		}
			values.add(new AbstractMap.SimpleEntry<>(key, v));
    	}
    	
    	RealtimeValueBroker.UpdateContext context = null;
    	try {
    		context = RealtimeValueBroker.prepareUpdate(false);
    		for(HubProxy p: targets){
    			//TODO pがnullでないように
    			if(p != null){
    				log.trace(p.getURI());
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
    
    protected void doCommon(HttpServletRequest req, HttpServletResponse resp, boolean write) {
    	final Map<String,String[]> parms = req.getParameterMap();

    	resp.setCharacterEncoding("UTF-8");
    	//String path = req.getServletPath();
    	String path = "/dummy"+req.getPathInfo();
    	
    	RealtimeValueUtil.TargetOperation targetOperation;
    	try {
    		 targetOperation = RealtimeValueUtil.findTargets(path,
    				 (index)->{
    					 String[] result =parms.get(index);
    					 return result==null?null:Arrays.asList(result);
    				 });
    	}catch(DataFormatException e){
    		sendError(resp, HttpServletResponse.SC_METHOD_NOT_ALLOWED, e.toString());
    		return;
    	}
    	
    	if(targetOperation.getOperation() == RealtimeValueUtil.Operation.READ){
    		readOp(resp,targetOperation);
    	}
    	else {
    		writeOp(resp,targetOperation, parms);
    	}
    }
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
    	doCommon(req, resp, false);
    }
    
    @Override
    protected void doPut(HttpServletRequest req, HttpServletResponse resp) {
    	doCommon(req, resp, true);
    }
    
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) {
    	doCommon(req, resp, true);
    }
    

    @Override
    protected void doHead(HttpServletRequest req, HttpServletResponse resp) {
        //doCommon(req, resp); 
    }
}
