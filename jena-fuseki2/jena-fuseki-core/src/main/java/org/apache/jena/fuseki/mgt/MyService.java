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

package org.apache.jena.fuseki.mgt;

import static org.apache.jena.riot.WebContent.charsetUTF8 ;
import static org.apache.jena.riot.WebContent.contentTypeTextPlain ;

import java.io.IOException ;
import java.util.Iterator;

import javax.servlet.ServletOutputStream ;
import javax.servlet.http.HttpServletRequest ;
import javax.servlet.http.HttpServletResponse ;

import org.apache.jena.atlas.io.IndentedLineBuffer;
import org.apache.jena.atlas.lib.DateTimeUtils ;
import org.apache.jena.atlas.lib.Lib;
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

// MNakagawa
/** The ping servlet provides a low costy, uncached endpoint that can be used
 * to determine if this component is running and responding.  For example,
 * a nagios check should use this endpoint.    
 */
@SuppressWarnings("serial")
public class MyService extends WebSocketServlet
{
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
		// Listener
		factory.register(MyWebSocket.class);
	}

	// Ping is special.
    // To avoid excessive logging and id allocation for a "noise" operation,
    // this is a raw servlet.
    public MyService() { super() ; } 
    
    @Override
    protected void doGet(HttpServletRequest req, HttpServletResponse resp) {
        doCommon(req, resp); 
    }
    
    @Override
    protected void doPost(HttpServletRequest req, HttpServletResponse resp) {
        doCommon(req, resp); 
    }
    

    @Override
    protected void doHead(HttpServletRequest req, HttpServletResponse resp) {
        doCommon(req, resp); 
    }

    //String queryString ="SELECT * WHERE { ?a ?b ?c . }"; 
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
    
    private void test(){
        DataAccessPoint dataAccessPoint ;
        DataService dSrv ;
        
        String datasetUri = "/test01";
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
        	Dataset dataset = DatasetFactory.create(dSrv.getDataset());
            try ( QueryExecution qExec = QueryExecutionFactory.create(query, dataset) ; ) {
            	ResultSet rs = qExec.execSelect() ;            	
                //SPARQLResult result = executeQuery(action, qExec, query, queryStringLog) ;
                // Deals with exceptions itself.
                //sendResults(action, result, query.getPrologue()) ;
            	System.out.println("RS: "+rs.toString());
            	while(rs.hasNext()){
            		QuerySolution s = rs.next();
            		Iterator<String> n = s.varNames();
            		while(n.hasNext()){
            			String name = n.next();
            			RDFNode nd = s.get(name);
            			System.out.println(name+":"+nd.toString());
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
            Fuseki.serverLog.warn("ping :: IOException :: "+ex.getMessage());
        }
    }
}


