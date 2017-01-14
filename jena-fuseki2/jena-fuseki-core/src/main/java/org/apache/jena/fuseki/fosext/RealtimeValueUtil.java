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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.fosext.FosNames;
import org.apache.jena.fosext.HubProxy;
import org.apache.jena.fosext.LeafValue;
import org.apache.jena.fosext.RealtimeValueBroker;
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
	private static final String HISTORY_SPECIFIER =PARM_ESCAPE_PREFIX+"history";
	private static final String LATEST_SPECIFIER =PARM_ESCAPE_PREFIX+"latest";
	private static final String PROPERTY_NAMESPACE_SPECIFIER =PARM_ESCAPE_PREFIX+"propertyns";

	private static final String DEFAULT_PROXY_NAMESPACE = FosNames.FOS_TAG_BASE;
	//private static final String DEFAULT_PROPERTY_NAMESPACE = FosNames.FOS_NAME_BASE;
	private static final String DEFAULT_PROPERTY_NAMESPACE = FosNames.FOS_TAG_BASE;
	private static final String LINK_ANY = "*";
	private static final String LINK_SEPARATOR = "-";
	private static final Pattern LINK_TAG_PAIR = Pattern.compile("(.+)"+LINK_SEPARATOR+"(.+)");

	
	
	private static final String HUB = "hub";
	private static final String QUERY_HEAD =
//			"PREFIX :        <"+RealtimeValueBroker.FOS_NAME_BASE+">\n"+ 
			"SELECT ?hub\n"+
			"WHERE {\n";

	//private static final String QUERY_MIDDLE1 =
	//		"      ?s%d <"+PATH_TAG+"> <%s> .\n";
	private static final String QUERY_MIDDLE1 =
			"      ?s%d <"+FosNames.FOS_PATH_TAG+"> <%s> .\n";

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
	public static HubProxy[] findProxiesByPath(String datasetName, String[] pParts, int offset, GetParm parms) throws DataFormatException {
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
			log.debug("link:"+defaultLink);
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
		log.debug("query\n"+queryString.toString());
		List<Map<String,String>> result = SparqlAccess.execute(datasetName, queryString.toString());
		if(result.size() == 0){
			throw new DataFormatException("No path found");
		}
		
		// 結果を抽出する
		HubProxy[] ret = new HubProxy[result.size()];
		for(int j =0; j<result.size();++j){
			String id = result.get(j).get(HUB);
			ret[j] = RealtimeValueBroker.getHubProxy(id);
			/*
			if(ret[j] == null){
				log.error(String.format("Unknown id:%s", id));
			}
			*/
		}
		return ret;
	}
	
	public static HubProxy findProxyById(String label) throws DataFormatException {
		String id = FosNames.FOS_PROXY_HUB+label;
		HubProxy proxy = RealtimeValueBroker.getHubProxy(id);
		if(proxy == null){
			throw new DataFormatException(String.format("Unknown id:%s", id));
		}
		return proxy;
	}

	
	public static HubProxy[] findProxiesByQuery(String datasetName, GetParm parms) throws DataFormatException {
		List<String> queryArray = parms.get("-query");
		if(queryArray == null){
			throw new DataFormatException("No query parm.");
		}
		if(queryArray.size() != 1){
			throw new DataFormatException("Duplicate query parms.");
		}
		String query = queryArray.get(0);
		
		// クエリを実行する
		List<Map<String,String>> result = SparqlAccess.execute(datasetName, query);
		if(result.size() == 0){
			throw new DataFormatException("No path found");
		}
		
		// 結果を抽出する
		HubProxy[] ret = new HubProxy[result.size()];
		for(int j =0; j<result.size();++j){
			Map<String,String> ansset = result.get(j);
			if(ansset.size() != 1){
				throw new DataFormatException("Multiple result elements??");
			}
			String id = result.get(j).values().iterator().next();
			ret[j] = RealtimeValueBroker.getHubProxy(id);
			if(ret[j] == null){
				log.debug(String.format("Unknown id:%s", id));
			}
		}
		return ret;
	}

	private static HubProxy[] findProxies(String datasetName, Operation operation, String[] pParts, int offset, GetParm parms) throws DataFormatException {
		String selector = pParts[offset];
		switch(selector){
		case "path":
			return findProxiesByPath(datasetName, pParts, offset+1, parms);

		case "id":
			if(pParts.length-offset != 2){
				throw new DataFormatException("Specify a id.");
			}
			HubProxy[] proxy = new HubProxy[1]; 
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
		private int history = -1; // 負なら指定なし
		private boolean latest = false; // trueなら現在値を返す
		private Map<String,HubProxy> targets;
		private HubProxy[] targetArray;
		private String propertyNamespace; // hubからproxyを指すpredicateの名前空間

		public Operation getOperation() {
			return operation;
		}
		
		public void setOperation(Operation operation) {
			this.operation = operation;
		}
		
		public Map<String,HubProxy> getTargets() {
			return targets;
		}
		
		public HubProxy[] getTargetArray(){
			return this.targetArray;
		}
		
		public HubProxy getTargetById(String id){
			return this.targets.get(id);
		}
		
		public void setTargets(HubProxy[] proxies) {
			this.targetArray = proxies;
			this.targets = new HashMap<>();
			for(HubProxy p: proxies){
				if(p != null){
					this.targets.put(p.getIdStr(), p);
				}
			}
		}
		
		public int getHistory(){
			return this.history;
		}
		
		public boolean getLatest(){
			return this.latest;
		}
		
		public String getPropertyNamespace(){
			return this.propertyNamespace;
		}
	}
	
	@FunctionalInterface
	public interface GetParm {
		public List<String> get(String key);
	}
	
	public static TargetOperation findTargets(String path, GetParm parms) throws DataFormatException {
		try {
			TargetOperation ret = new TargetOperation();

			//List<String> test = parms.get("test");
			String epath;
			try {
				epath = URLDecoder.decode(path, "UTF-8");
			}
			catch (UnsupportedEncodingException e ){
				throw new DataFormatException("Illegal path format:"+path);
			}

			List<String> historyParm = parms.get(HISTORY_SPECIFIER); 
			if(historyParm != null){
				ret.history = Integer.valueOf(historyParm.get(0));
			}

			List<String> latestParm = parms.get(LATEST_SPECIFIER);
			if(latestParm != null && Integer.valueOf(latestParm.get(0)) != 0){
				// 値にかかわらず
				ret.latest = true;
			}

			List<String> propertyNsParm = parms.get(PROPERTY_NAMESPACE_SPECIFIER);
			if(propertyNsParm != null ){
				ret.propertyNamespace = propertyNsParm.get(0);
			}
			else {
				ret.propertyNamespace = FosNames.FOS_TAG_BASE;
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
		} catch(Exception e){
			throw new DataFormatException(String.format("Parm error:%s", e.toString()));
		}
	}

	private final static Pattern INT_PAT = Pattern.compile("[\\-+]?\\d+");
	private final static Pattern FLOAT_PAT = Pattern.compile("[\\-+]?\\d+\\.\\d*");//ちゃんとつくってない
	private final static String FLOAT_PAT_EXCEPT = ".";

	public static LeafValue str2value(String valueStr){
		LeafValue ret;
		// TODO 値のパーズちゃんとしていない
		Matcher im = INT_PAT.matcher(valueStr);
		if(im.matches()){
			Long i = Long.valueOf(valueStr);
			ret = new LeafValue(i, XSDDatatype.XSDint); 
		}
		else {
    		Matcher fm = FLOAT_PAT.matcher(valueStr);
			if(fm.matches() && !valueStr.equals(FLOAT_PAT_EXCEPT)){
    			Double f = Double.valueOf(valueStr);
    			ret = new LeafValue(f, XSDDatatype.XSDdouble); 
			}
			else {
    			ret = new LeafValue(valueStr, XSDDatatype.XSDstring);
			}
		}
    	return ret;
    }	
}
