package org.apache.jena.fuseki.fosext;

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

public class RealtimeValueUtil {
	private final static Pattern PATH_PATTERN = Pattern.compile("/");

	public static enum Operation {
		UPDATE,
		READ,
		;
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
	
	private static RealtimeValueBroker.HubProxy[] findProxies(Operation operation, String[] pParts, int offset) throws DataFormatException {
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
	
		List<String> test = parms.get("test");
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
			ret.setOperation(Operation.valueOf(pParts[2].toUpperCase()));
		}
		catch(IllegalArgumentException e){
			throw new DataFormatException(String.format("Unknown operation:%s:%s", pParts[2], epath));
		}

		try {
			ret.setTargets(findProxies(ret.operation, pParts, 3));
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
	
	public static String getRealtimeValue(RealtimeValueBroker.HubProxy proxy){
		List<RealtimeValueBroker.Pair<String,Node>> values = proxy.getPropertyValuePairs();
		StringBuilder ret = new StringBuilder();
		ret.append('{');
		for(int i = 0;;){
			RealtimeValueBroker.Pair<String,Node> p = values.get(i);
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
					ret.append(escape(o.toString()));
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
		return ret.toString();
	}
	
    private final static Pattern INT_PAT = Pattern.compile("^[0-9]+$");
    private final static Pattern FLOAT_PAT = Pattern.compile("^[0-9]*\\.[0-0]*$");
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
