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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.zip.DataFormatException;

import org.apache.jena.atlas.lib.Lib;
import org.apache.jena.fuseki.Fuseki;
import org.apache.jena.fuseki.server.DataAccessPoint;
import org.apache.jena.fuseki.server.DataAccessPointRegistry;
import org.apache.jena.fuseki.server.DataService;
import org.apache.jena.fuseki.servlets.ActionErrorException;
import org.apache.jena.fuseki.servlets.ServletOps;
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

/**
 * @author m-nakagawa
 *
 */
public class SparqlAccess {
    public static List<Map<String,String>> execute(String datasetName, String queryText) throws DataFormatException {
    	datasetName = "/"+datasetName;
    	List<Map<String,String>> ret = new ArrayList<>();
        DataAccessPoint dataAccessPoint ;
        DataService dSrv ;
        
        dataAccessPoint = DataAccessPointRegistry.get().get(datasetName) ;
        if ( dataAccessPoint == null ) {
        	throw new DataFormatException("No dataset: "+datasetName) ;
        }
        dSrv = dataAccessPoint.getDataService() ;
        if ( ! dSrv.isAcceptingRequests() ) {
        	throw new DataFormatException("Dataset not active: "+datasetName) ;
        }
    	
        Query query = null ;
        try {
            // NB syntax is ARQ (a superset of SPARQL)
            query = QueryFactory.create(queryText, Fuseki.BaseParserSPARQL, Syntax.syntaxARQ) ;
            //String queryStringLog = formatForLog(query) ;
            //System.err.println(queryStringLog);
            //validateQuery(action, query) ;
        } catch (ActionErrorException ex) {
            throw ex ;
        } catch (QueryParseException ex) {
            ServletOps.errorBadRequest("Parse error: \n" + queryText + "\n\r" + messageForQueryException(ex)) ;
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
            	//System.err.println("RS: "+rs.toString());
            	while(rs.hasNext()){
            		QuerySolution s = rs.next();
            		Iterator<String> n = s.varNames();
            		Map<String,String> map = new HashMap<>();
            		ret.add(map);
            		while(n.hasNext()){
            			String name = n.next();
            			RDFNode nd = s.get(name);
            			map.put(name, nd.toString());
            		}
            	}
            }
        } 
        catch (QueryParseException ex) {
            // Late stage static error (e.g. bad fixed Lucene query string). 
            ServletOps.errorBadRequest("Query parse error: \n" + queryText + "\n\r" + messageForQueryException(ex)) ;
        }
        catch (QueryCancelledException ex) {
            // Additional counter information.
            //incCounter(action.getEndpoint().getCounters(), QueryTimeouts) ;
            throw ex ;
        } finally {}
        return ret;
    }
    
    private static String messageForQueryException(QueryException ex) {
        if ( ex.getMessage() != null )
            return ex.getMessage() ;
        if ( ex.getCause() != null )
            return Lib.classShortName(ex.getCause().getClass()) ;
        return null ;
    }

    /*
    private static String formatForLog(Query query) {
        IndentedLineBuffer out = new IndentedLineBuffer() ;
        out.setFlatMode(true) ;
        query.serialize(out) ;
        return out.asString() ;
    }
    */
}
