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

import static org.apache.jena.riot.WebContent.charsetUTF8 ;
import static org.apache.jena.riot.WebContent.contentTypeTextPlain ;
import static org.slf4j.LoggerFactory.getLogger;

import java.io.IOException ;
import java.io.PrintWriter;
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

import org.apache.jena.atlas.io.IndentedLineBuffer;
import org.apache.jena.atlas.lib.DateTimeUtils ;
import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.fosext.RealtimeValueBroker;
import org.apache.jena.fuseki.Fuseki ;
import org.apache.jena.fuseki.server.DataAccessPoint;
import org.apache.jena.fuseki.server.DataAccessPointRegistry;
import org.apache.jena.fuseki.server.DataService;
import org.apache.jena.fuseki.servlets.ActionErrorException;
import org.apache.jena.fuseki.servlets.ServletOps ;
import org.apache.jena.query.Dataset;
import org.apache.jena.query.DatasetFactory;
import org.apache.jena.query.Query;
import org.apache.jena.query.QueryCancelledException;
import org.apache.jena.query.QueryException;
import org.apache.jena.query.QueryExecution;
import org.apache.jena.query.QueryExecutionFactory;
import org.apache.jena.query.QueryFactory;
import org.apache.jena.query.QueryParseException;
import org.apache.jena.query.QuerySolution;
import org.apache.jena.query.ResultSet;
import org.apache.jena.query.Syntax;
import org.apache.jena.rdf.model.RDFNode;
import org.apache.jena.web.HttpSC ;
import org.eclipse.jetty.websocket.servlet.WebSocketServlet;
import org.eclipse.jetty.websocket.servlet.WebSocketServletFactory;
import org.slf4j.Logger;

// MNakagawa
/** The ping servlet provides a low costy, uncached endpoint that can be used
 * to determine if this component is running and responding.  For example,
 * a nagios check should use this endpoint.    
 */
@SuppressWarnings("serial")
public class MyService extends WebSocketServlet
{
    private final static Logger log = getLogger(MyService.class);
	/*
	@WebSocket
	public static class WebSocketSample {
	    private Session session;

	    @OnWebSocketConnect
	    public void onConnect(Session session) {
	        this.session = session;
	        System.out.println("Connect");
	        try {
	        	this.session.getBasicRemote().sendText("Hello.");
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
	    	System.out.println("Close");
	    }
	}
*/
	@Override
	public void configure(WebSocketServletFactory factory) {
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

    private void readOp(HttpServletResponse resp, RealtimeValueBroker.HubProxy[] targets){
    	try{
    		PrintWriter writer = resp.getWriter();
    		writer.println("[");
    		boolean prev = false;
    		for(RealtimeValueBroker.HubProxy p: targets){
    			//TODO pがnullを含まないように
    			if(p != null){
    				if(prev){
    					writer.print(",\n");
    				}
    				writer.print(RealtimeValueUtil.getRealtimeValue(p));
    				prev = true;
    			}
    		}
    		writer.println("\n]");
    	} catch (IOException ex) {
    		Fuseki.serverLog.warn("myservice :: IOException :: "+ex.getMessage());
    	}
    }
    

    
    private void writeOp(HttpServletResponse resp, RealtimeValueBroker.HubProxy[] targets, Map<String,String[]> parms){
    	List<RealtimeValueBroker.Pair<String, RealtimeValueBroker.Value>> values = new ArrayList<>(); 
    	for(Entry<String,String[]> e: parms.entrySet()){
    		String key = e.getKey();
    		if(RealtimeValueUtil.isEscapeParm(key)){
    			//制御パラメータ
    			continue;
    		}

    		String[] vs = e.getValue();
    		String valueStr;
    		if(vs.length == 1){
    			//正しいパラメータ
        		valueStr = e.getValue()[0];
        		key = RealtimeValueBroker.FOS_NAME_BASE+key;
    		}
    		/*
    		else if(vs.length == 0){
    			//値名省略パラメータ
    			valueStr = key;
    			key = RealtimeValueBroker.FOS_DEFAULT_VALUE_TAG;
    		}
    		*/
    		else {
        		Fuseki.serverLog.error("Duplicate param:"+key);
        		continue;
    		}
    		
    		System.err.println("xxxx "+key+"  "+valueStr);
    		RealtimeValueBroker.Value value = RealtimeValueUtil.str2value(valueStr);
			values.add(new RealtimeValueBroker.Pair<>(key, value));
    	}
    	
    	RealtimeValueBroker.UpdateContext context = null;
    	try {
    		context = RealtimeValueBroker.prepareUpdate();
    		for(RealtimeValueBroker.HubProxy p: targets){
    			//TODO pがnullでないように
    			if(p != null){
    				System.err.println("---"+p.getURI());
    				for(RealtimeValueBroker.Pair<String, RealtimeValueBroker.Value> e: values){
    					p.setValue(e.getKey(), e.getValue(), context.getInstant());
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
    		readOp(resp,targetOperation.getTargets());
    	}
    	else {
    		writeOp(resp,targetOperation.getTargets(), parms);
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

    String queryString ="SELECT * WHERE { ?a ?b ?c . }";
    /*
    String queryString="#日本語\n"
    		+ "PREFIX : <http://bizar.aitc.jp/ns/fos/0.1/>\n"
    		+ "PREFIX ha: <http://bizar.aitc.jp/ns/fos/0.1/人間API/>\n"
    		+ "#DELETE{ ?c :value        ?data . }\n"	
    		+ "INSERT{ ?c :value         999 . }\n"
    		+ "WHERE {\n"
    		+ "?c  :value		?data .\n"
			+ "}\n";
	*/
	/*
	 * 
    String queryString = "#日本語\n"
    		+"PREFIX : <http://bizar.aitc.jp/ns/fos/0.1/>\n"
    		+"PREFIX ha: <http://bizar.aitc.jp/ns/fos/0.1/人間API/>\n"
    		+"SELECT ?url\n"
    		+"WHERE {\n"
    		+"?c ha:テキストメッセージ ?url .\n"
    		+"?c :主人 ?person .\n"
    		+"{\n"
    		+"SELECT ?person\n" 
    		+"WHERE {\n"
    		+"?room :場所 :ここ .\n"
    		+"?room :在室 ?person .\n"
    		+"}}}\n"
    		;
    */
    private void test(){
        DataAccessPoint dataAccessPoint ;
        DataService dSrv ;
        
        String datasetUri = "/test01";
        log.debug("test");
        dataAccessPoint = DataAccessPointRegistry.get().get(datasetUri) ;
        if ( dataAccessPoint == null ) {
        	System.err.println("No dataset for URI: "+datasetUri) ;
        	return ;
        }
        dSrv = dataAccessPoint.getDataService() ;
        if ( ! dSrv.isAcceptingRequests() ) {
        	System.err.println("Dataset not active: "+datasetUri) ;
        	return ;
        }
    	
        Query query = null ;
        try {
            // NB syntax is ARQ (a superset of SPARQL)
            query = QueryFactory.create(queryString, Fuseki.BaseParserSPARQL, Syntax.syntaxARQ) ;
            String queryStringLog = formatForLog(query) ;
            System.err.println(queryStringLog);
            //validateQuery(action, query) ;
        } catch (ActionErrorException ex) {
            throw ex ;
        } catch (QueryParseException ex) {
            ServletOps.errorBadRequest("Parse error: \n" + queryString + "\n\r" + messageForQueryException(ex)) ;
        }
        
        try {
            //action.beginRead() ;
            //Dataset dataset = decideDataset(action, query, queryStringLog) ;
        	Dataset dataset = DatasetFactory.wrap(dSrv.getDataset());
            try ( QueryExecution qExec = QueryExecutionFactory.create(query, dataset) ; ) {
            	ResultSet rs = qExec.execSelect() ;            	
                //SPARQLResult result = executeQuery(action, qExec, query, queryStringLog) ;
                // Deals with exceptions itself.
                //sendResults(action, result, query.getPrologue()) ;
            	System.err.println("RS: "+rs.toString());
            	while(rs.hasNext()){
            		QuerySolution s = rs.next();
            		Iterator<String> n = s.varNames();
            		while(n.hasNext()){
            			String name = n.next();
            			RDFNode nd = s.get(name);
            			System.err.println(name+":"+nd.toString());
            		}
            	}
            }
        } 
        catch (QueryParseException ex) {
            // Late stage static error (e.g. bad fixed Lucene query string). 
            ServletOps.errorBadRequest("Query parse error: \n" + queryString + "\n\r" + messageForQueryException(ex)) ;
        }
        catch (QueryCancelledException ex) {
            // Additional counter information.
            //incCounter(action.getEndpoint().getCounters(), QueryTimeouts) ;
            throw ex ;
        } finally {}

    }
    
    protected static String messageForQueryException(QueryException ex) {
        if ( ex.getMessage() != null )
            return ex.getMessage() ;
        if ( ex.getCause() != null )
            return Lib.classShortName(ex.getCause().getClass()) ;
        return null ;
    }

    private String formatForLog(Query query) {
        IndentedLineBuffer out = new IndentedLineBuffer() ;
        out.setFlatMode(true) ;
        query.serialize(out) ;
        return out.asString() ;
    }
    
    protected void doCommon(HttpServletRequest request, HttpServletResponse response) {
        try {
        	test();
            ServletOps.setNoCache(response) ; 
            response.setContentType(contentTypeTextPlain);
            response.setCharacterEncoding(charsetUTF8) ;
            response.setStatus(HttpSC.OK_200);
            ServletOutputStream out = response.getOutputStream() ;
            out.println(DateTimeUtils.nowAsXSDDateTimeString());
        } catch (IOException ex) {
            Fuseki.serverLog.warn("myservice :: IOException :: "+ex.getMessage());
        }
    }
}


