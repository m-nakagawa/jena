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
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.fosext.RealtimeValueBroker;
import org.apache.jena.graph.Node;
import org.slf4j.Logger;

/**
 * @author m-nakagawa
 *
 */
public class RealtimeValueUtil {
    private final static Logger log = getLogger(RealtimeValueUtil.class);
	
	private final static Pattern PATH_PATTERN = Pattern.compile("/");

	public static enum Operation {
		UPDATE,
		READ,
		;
	}

	private static final char PARM_ESCAPE = '-';
	public static final String PARM_ESCAPE_PREFIX = String.valueOf(PARM_ESCAPE);
	
	public static final boolean isEscapeParm(String s){
		if(s.charAt(0) == PARM_ESCAPE){
			return true;
		}
		else {
			return false;
		}
	}
	
	private static final String LINK_SPECIFIER =PARM_ESCAPE_PREFIX+"link";
	private static final String PATH_TAG = RealtimeValueBroker.FOS_NAME_BASE+"パス識別子";
	private static final String DEFAULT_PROXY_NAMESPACE = RealtimeValueBroker.FOS_TAG_BASE;
	private static final String DEFAULT_PROPERTY_NAMESPACE = RealtimeValueBroker.FOS_NAME_BASE;
	private static final String LINK_ANY = "*";
	private static final String LINK_SEPARATOR = "-";
	private static final Pattern LINK_TAG_PAIR = Pattern.compile("(.+)"+LINK_SEPARATOR+"(.+)");

	
	
	private static final String HUB = "hub";
	private static final String QUERY_HEAD =
//			"PREFIX :        <"+RealtimeValueBroker.FOS_NAME_BASE+">\n"+ 
			"SELECT ?hub\n"+
			"WHERE {\n";

	private static final String QUERY_MIDDLE1 =
			"      ?s%d <"+PATH_TAG+"> <%s> .\n";
	
	private static final String QUERY_MIDDLE2 =
			"      ?s%d %s ?s%d .\n" ;

	private static final String QUERY_TAIL =
			"      ?s%d <%s> ?"+HUB+" .\n"+
			"}\n";

	private static String linkAny(int i){
		return "?t"+Integer.valueOf(i);
	}
	
	/**
	 * タグパスにマッチする物理ノードを検索する
	 *  <pre>
	 *   /fos/<dataset>/<update|read>/path/<tag0>/<tag1>/.../<tagN>?link=構成
	 *   pParts[offset] : <dataset>
	 *  </pre>
	 * @param pParts
	 * @param offset
	 * @return
	 * @throws DataFormatException
	 */
	public static RealtimeValueBroker.HubProxy[] findProxiesByPath(String datasetName, String[] pParts, int offset, GetParm parms) throws DataFormatException {
		//TODO 返り値にnullを含むことがあるので、Listを返すようにする。
		int pathLength = pParts.length-offset-1;
		
		if(pathLength < 0){
			throw new DataFormatException("No path specified");
		}

		String partsNamespace = DEFAULT_PROXY_NAMESPACE;
		String propertyNamespace = DEFAULT_PROPERTY_NAMESPACE;
		String defaultLink = null;
		
		if(parms.get(LINK_SPECIFIER) != null){
			defaultLink = parms.get(LINK_SPECIFIER).get(0);
			System.err.println("link:"+defaultLink);
			if(defaultLink.equals(LINK_ANY)){
				defaultLink = null;
			}
		}
		
		//String datasetName = pParts[offset];

		// SPARQLクエリを生成する
		int anyCnt = 0;
		StringBuilder queryString = new StringBuilder(QUERY_HEAD);

		int part = offset;
		for(int i = 0; i < pathLength; ++i, ++part){
			Matcher m = LINK_TAG_PAIR.matcher(pParts[part]);
			String node;
			String link;
			if(i != 0 && m.matches()){
				link = m.group(1);
				if(link.equals(LINK_ANY)){
					link = linkAny(anyCnt++);
				}
				else {
					if(defaultLink == null){
						link = linkAny(anyCnt++);
					}
					else {
						link = "<"+propertyNamespace+defaultLink+">";
					}
				}
				node = m.group(2);
			}
			else {
				if(defaultLink == null){
					link = linkAny(anyCnt++);
				}
				else {
					link = defaultLink;
				}
				node = pParts[part];
			}
			
			if(i != 0){
				queryString.append(String.format(QUERY_MIDDLE2, i-1, link, i));
			}
			queryString.append(String.format(QUERY_MIDDLE1, i, partsNamespace+node));
		}
		queryString.append(String.format(QUERY_TAIL, pathLength==0?0:pathLength-1, propertyNamespace+pParts[part]));

		// クエリを実行する
		System.err.println(queryString.toString());
		List<Map<String,String>> result = SparqlAccess.execute(datasetName, queryString.toString());
		if(result.size() == 0){
			throw new DataFormatException("No path found");
		}
		
		// 結果を抽出する
		RealtimeValueBroker.HubProxy[] ret = new RealtimeValueBroker.HubProxy[result.size()];
		for(int j =0; j<result.size();++j){
			String id = result.get(j).get(HUB);
			ret[j] = RealtimeValueBroker.getRootProxy(id);
			/*
			if(ret[j] == null){
				log.error(String.format("Unknown id:%s", id));
			}
			*/
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

	
	public static RealtimeValueBroker.HubProxy[] findProxiesByQuery(String datasetName, GetParm parms) throws DataFormatException {
		List<String> queryArray = parms.get("-query");
		if(queryArray == null){
			throw new DataFormatException("No query parm.");
		}
		if(queryArray.size() != 1){
			throw new DataFormatException("Duplicate query parms.");
		}
		String query = queryArray.get(0);
		
		// クエリを実行する
		System.err.println(query);
		List<Map<String,String>> result = SparqlAccess.execute(datasetName, query);
		if(result.size() == 0){
			throw new DataFormatException("No path found");
		}
		
		// 結果を抽出する
		RealtimeValueBroker.HubProxy[] ret = new RealtimeValueBroker.HubProxy[result.size()];
		for(int j =0; j<result.size();++j){
			Map<String,String> ansset = result.get(j);
			if(ansset.size() != 1){
				throw new DataFormatException("Multiple result elements??");
			}
			String id = result.get(j).values().iterator().next();
			ret[j] = RealtimeValueBroker.getRootProxy(id);
			/*
			if(ret[j] == null){
				log.error(String.format("Unknown id:%s", id));
			}
			*/
		}
		return ret;
	}

	private static RealtimeValueBroker.HubProxy[] findProxies(String datasetName, Operation operation, String[] pParts, int offset, GetParm parms) throws DataFormatException {
		String selector = pParts[offset];
		switch(selector){
		case "path":
			return findProxiesByPath(datasetName, pParts, offset+1, parms);

		case "id":
			if(pParts.length-offset != 2){
				throw new DataFormatException("Specify a id.");
			}
			RealtimeValueBroker.HubProxy[] proxy = new RealtimeValueBroker.HubProxy[1]; 
			proxy[0] = findProxyById(pParts[offset+1]); 
			return proxy;

		case "query":
			if(pParts.length-offset != 1){
				throw new DataFormatException("Too many path elements.");
			}
			return findProxiesByQuery(datasetName, parms);
			
		default:
			throw new DataFormatException(String.format("Undefined selector:%s", selector));
		}
	}

	public static class TargetOperation {
		private Operation operation;
		private RealtimeValueBroker.HubProxy[] targets;

		public Operation getOperation() {
			return operation;
		}
		public void setOperation(Operation operation) {
			this.operation = operation;
		}
		public RealtimeValueBroker.HubProxy[] getTargets() {
			return targets;
		}
		public void setTargets(RealtimeValueBroker.HubProxy[] targets) {
			this.targets = targets;
		}
	}
	
	@FunctionalInterface
	public interface GetParm {
		public List<String> get(String key);
	}
	
//	public static TargetOperation findTargets(String path, Map<String,List<String>> parms) throws DataFormatException {
	public static TargetOperation findTargets(String path, GetParm parms) throws DataFormatException {
		TargetOperation ret = new TargetOperation();
	
		//List<String> test = parms.get("test");
		String epath;
		try {
			epath = URLDecoder.decode(path, "UTF-8");
		}
		catch (UnsupportedEncodingException e ){
			throw new DataFormatException("Illegal path format:"+path);
		}
		
		// /fos/<dataset>/(update|read)/(path|id|query)/....
		String[] pParts = PATH_PATTERN.split(epath);
		if(pParts.length < 4 ){
			throw new DataFormatException("Illegal path format:"+epath);
		}

		String datasetName = pParts[2];
		try {
			ret.setOperation(Operation.valueOf(pParts[3].toUpperCase()));
		}
		catch(IllegalArgumentException e){
			throw new DataFormatException(String.format("Unknown operation:%s:%s", pParts[3], epath));
		}

		try {
			ret.setTargets(findProxies(datasetName, ret.operation, pParts, 4, parms));
		}
		catch(DataFormatException e){
			throw new DataFormatException(String.format("%s:%s", e.toString(), epath));
		}
		
		return ret;
	}

	private static final Pattern STRING_ESCAPE = Pattern.compile("([\"\\\\])");
	private static final String  STRING_ESCAPE_REPLACE = "\\\\$0"; 
	private static String escape(String s){
		Matcher m = STRING_ESCAPE.matcher(s);
		if(m.find()){
			return m.replaceAll(STRING_ESCAPE_REPLACE);
		}
		else {
			return s;
		}
	}

	private static void expandValues(StringBuilder ret, RealtimeValueBroker.LeafProxy proxy){
		if(proxy.isArray()){
			ret.append('[');
		}
		boolean first = true;
		Node nodes[] = proxy.getCurrentValue();
		for(Node n : nodes){
			if(first){
				first = false;
			}
			else {
				ret.append(',');
			}
			
			if(n.isLiteral()){
				Object o = n.getLiteralValue();
				if(o instanceof Number){
					ret.append(o.toString());
				}
				else {
					ret.append('"');
					ret.append(escape(o.toString()));
					ret.append('"');
				}
			}
			else {
				ret.append('"');
				ret.append(n.toString());
				ret.append('"');
			}
		}
		if(proxy.isArray()){
			ret.append(']');
		}
	}
	
	public static String getRealtimeValue(RealtimeValueBroker.HubProxy proxy){
		List<RealtimeValueBroker.Pair<String,RealtimeValueBroker.LeafProxy>> values = proxy.getPropertyValuePairs();
		StringBuilder ret = new StringBuilder();
		ret.append('{');
		ret.append("\""+RealtimeValueBroker.FOS_PROXY_ID+"\":\"");
		ret.append(escape(proxy.getIdStr()));
		ret.append("\",");

		for(int i = 0;;){
			RealtimeValueBroker.Pair<String,RealtimeValueBroker.LeafProxy> p = values.get(i);
			ret.append('"');
			ret.append(p.getKey());
			ret.append("\":");
			expandValues(ret, p.getValue());
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
		return ret.toString();
	}
	
    private final static Pattern INT_PAT = Pattern.compile("[\\-+]?\\d+");
    private final static Pattern FLOAT_PAT = Pattern.compile("[\\-+]?\\d+\\.\\d*");//ちゃんとつくってない
    private final static String FLOAT_PAT_EXCEPT = ".";

    public static RealtimeValueBroker.Value str2value(String valueStr){
    	RealtimeValueBroker.Value ret;
		// TODO 値のパーズちゃんとしていない
		Matcher im = INT_PAT.matcher(valueStr);
		if(im.matches()){
			Long i = Long.valueOf(valueStr);
			ret = new RealtimeValueBroker.Value(i, XSDDatatype.XSDint); 
		}
		else {
    		Matcher fm = FLOAT_PAT.matcher(valueStr);
			if(fm.matches() && !valueStr.equals(FLOAT_PAT_EXCEPT)){
    			Double f = Double.valueOf(valueStr);
    			ret = new RealtimeValueBroker.Value(f, XSDDatatype.XSDdouble); 
			}
			else {
    			ret = new RealtimeValueBroker.Value(valueStr, XSDDatatype.XSDstring);
			}
		}
    	return ret;
    }	
}
