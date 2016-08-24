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

package org.apache.jena.fosext;

import static org.slf4j.LoggerFactory.getLogger;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Node_URI;
import org.slf4j.Logger;

public class RealtimeValueBroker {
    private final static Logger log = getLogger(RealtimeValueBroker.class);

    public static final String FOS_NAME_BASE = "http://bizar.aitc.jp/ns/fos/0.1/";
	public static final String FOS_LOCAL_NAME_BASE = FOS_NAME_BASE+"local/";
	public static final String FOS_TAG_BASE = FOS_LOCAL_NAME_BASE+"label#";
	public static final String FOS_PROXY_BASE = FOS_LOCAL_NAME_BASE+"proxy/";
	public static final String FOS_PROXY_EMPTY = FOS_PROXY_BASE+"empty";
	public static final String FOS_PROXY_UNDEFINED = FOS_PROXY_BASE+"undefined";
	public static final String FOS_PROXY_HOLDER = FOS_PROXY_BASE+"holder#";
	public static final String FOS_PROXY_VALUE = FOS_PROXY_BASE+"value#";
	public static final String FOS_PROXY_DATETIME_SHORT = "時刻";
	public static final String FOS_PROXY_ID = "id";
	public static final String FOS_PROXY_DATETIME = FOS_NAME_BASE+FOS_PROXY_DATETIME_SHORT;
	private static final Pattern fosProxyHolder = Pattern.compile(FOS_PROXY_HOLDER+"(.*)$"); 
	private static final Pattern fosProxyValue = Pattern.compile(FOS_PROXY_VALUE+"(.*)$"); 
	
	private static Map<String,LeafProxy> leafProxyIndex = new HashMap<>();
	private static Map<String,HubProxy> rootProxyIndex = new HashMap<>();
	static {
		new TestProxy("http://bizar.aitc.jp/ns/fos/0.1/internal/01");
		// どれもデータ型を推測しない
		//LiteralLabel literal = LiteralLabelFactory.createLiteralLabel("123", "", (RDFDatatype)null);
		//LiteralLabel literal = LiteralLabelFactory.create("123", (RDFDatatype)null);
		//LiteralLabel literal = LiteralLabelFactory.createByValue("123", "", (RDFDatatype)null);
		//System.out.println(literal);
	}
	
	
	public static class TestProxy implements LeafProxy {
		private final String uri;
		private int value;
		
		private TestProxy(String uri){
			this.uri = uri;
			this.value = 0;
			addLeafProxy(this);
		}
		
		public String getURI(){
			return this.uri;
		}
		
		public void setCurrentValue(Value value){
			if(value.dtype == XSDDatatype.XSDint || value.dtype == XSDDatatype.XSDinteger){
				//this.value = Integer.valueOf(value.value);
				this.value = (Integer)value.value;
			}
			else {
				// 無視
			}
		}
		
		public Node getCurrentValue(){
			++value;
			return NodeFactory.createLiteral(Integer.toString(value), XSDDatatype.XSDinteger);
		}
	}



	// 全Proxyを一時停止
	public static void freeze(){
		System.out.println("FREEZE--------");
	}
	
	public static void release(){
		System.out.println("RELEASE--------");
	}
	
	private static Set<HubProxy> updatedProxies;
	private static LocalDateTime updateTime;

	public static void prepareUpdate(){
		updateTime = LocalDateTime.now();
		updatedProxies = new HashSet<>();
		System.out.println("PREPARE UPDATE--------");
	}
	
	public static void finishUpdate(){
		System.out.println("FINISH UPDATE--------");
		for(HubProxy proxy : updatedProxies){
			proxy.update(updateTime);
		}
	}
	
	// return true: Proxyがデータを消費した false:新規登録した
	public static boolean isProxy(Node g, Node s, Node p, Node o){
		System.err.println(String.format("%s:%s:%s:%s", g.toString(), s.toString(), p.toString(), o.toString()));
		if(s instanceof Node_URI ){
			Matcher mHolder = fosProxyHolder.matcher(s.toString());
			if(mHolder.matches()){
				if(o instanceof Node_URI){
					Matcher mValue = fosProxyValue.matcher(o.toString());
					if(mValue.matches()){
						// 登録 
						HubProxy root = getRootProxy(s.toString());
						if(root == null){
							root = new HubProxy(s.toString());
							addRootProxy(root);
						}
						LeafProxy leaf = getLeafProxy(o.toString());
						if(leaf == null){
							leaf = new ValueContainer(o.toString());
						}
						root.addLeaf(p.toString(), leaf);
						System.err.println(String.format("REGISTERED:%s:%s:%s", s.toString(), p.toString(), o.toString()));
						return false;
					}
				}
				else {
					// 値の設定
					HubProxy root = getRootProxy(s.toString());
					if(root != null){
						updatedProxies.add(root);
						root.setValue(p.toString(), o);
						return true;
					}
				}
			}
		}
		return false;
	}
	
	

	static class Value {
		private final Object value;
		private final String lang;
		private final RDFDatatype dtype;
		public Value(Object value, RDFDatatype dtype, String lang){
			this.value = value;
			this.lang = lang;
			this.dtype = dtype;
			//this.dtype = null;
		}
		
		public Value(Object value){
			this(value, XSDDatatype.XSDstring, null);
		}

		public Value(Object value, RDFDatatype dtype){
			this(value, dtype, null);
		}

		public Object getValue() {
			return value;
		}

		public String getLang() {
			return lang;
		}

		public RDFDatatype getDtype() {
			return dtype;
		}
	}

	public static final Pattern URI_SPLITTER = Pattern.compile("^(.*[#|/])(.+)$");
	
	public interface LeafProxy {
		String getURI();
		void setCurrentValue(Value value);
		Node getCurrentValue();
	}
	
	static void addLeafProxy(LeafProxy proxy){
		leafProxyIndex.put(proxy.getURI(), proxy);
	}
	
	public static LeafProxy getLeafProxy(String name){
		return leafProxyIndex.get(name);
	}
	
	static void addRootProxy(HubProxy proxy){
		rootProxyIndex.put(proxy.getURI(), proxy);
	}
	
	public static HubProxy getRootProxy(String name){
		return rootProxyIndex.get(name);
	}

	static class ValueContainer implements LeafProxy {
		private final String uri;
		private Value value;
		private ValueContainer(String uri){
			this.uri = uri;
			this.value = null;
			addLeafProxy(this);
		}
		
		public String getURI(){
			return this.uri;
		}
		
		public void setCurrentValue(Value value){
			this.value = value;
		}
		
		public Node getCurrentValue(){
			if(value != null){
				return NodeFactory.createLiteralByValue(value.getValue(), value.getLang(), value.getDtype());
			}
			else {
				return NodeFactory.createURI(FOS_PROXY_EMPTY);
			}
		}
	}
	
	public interface ValueConsumer {
		boolean informValueUpdate(HubProxy proxy); // return false: 登録を解除する
	}
	
	public final static class KVPair {
	    private final String key;
	    private final Node value;

	    public KVPair(String key, Node value) {
	        this.key = key;
	        this.value = value;
	    }

	    public String getKey() {
	        return key;
	    }

	    public Node getValue() {
	        return value;
	    }
	}
	

	public static class HubProxy {
		private final String uri;
		private final Node id;
		private final Map<String,LeafProxy> proxies;
		private final Set<ValueConsumer> consumers;
		private boolean datetimeIncluded = false; // true:setValueで日付時刻が設定された。 この値はsetDateTimeでfaluseに戻る。
		private LeafProxy datetime;
		private List<String> propertyNames;

		public HubProxy(String uri){
			this.uri = uri;
			this.proxies =  new HashMap<>();
			this.datetime = null;
			this.consumers = new HashSet<>();
			this.propertyNames = new ArrayList<>();
			Matcher m = URI_SPLITTER.matcher(uri);
			String idStr;
			if(m.matches()){
				idStr = m.group(2);
			}
			else {
				idStr = uri;
			}
			this.id = NodeFactory.createLiteral(idStr, "", XSDDatatype.XSDstring);
		}

		public void addConsumer(ValueConsumer consumer){
			this.consumers.add(consumer);
		}
		
		private void removeConsumer(ValueConsumer consumer){
			this.consumers.remove(consumer);
		}
		
		public String getURI() {
			return this.uri;
		}

		public void addLeaf(String predicate, LeafProxy leaf){
			String leafUri = leaf.getURI();
			Matcher m = RealtimeValueBroker.URI_SPLITTER.matcher(predicate);
			String errmsg = "";
			if(m.matches()){
				String shortPredicate = m.group(2);
				if(!this.proxies.containsKey(uri) && !this.proxies.containsKey(shortPredicate)){
					this.proxies.put(predicate, leaf);
					this.proxies.put(shortPredicate, leaf);
					if(!shortPredicate.equals(FOS_PROXY_DATETIME_SHORT)){
						this.propertyNames.add(shortPredicate);
						Collections.sort(this.propertyNames);
					}
				}
				errmsg = String.format("(%s)", shortPredicate);
			}
			log.error(String.format("Can't append %s to %s %s %s", leafUri, predicate, this.uri, errmsg));
		}
		
		public void update(LocalDateTime datetime){
			if(datetimeIncluded){
				// もし報告者からの時刻報告がすでに設定されているならそちらを優先する
				datetimeIncluded = false;
			}
			else {
				if(this.datetime == null){
					this.datetime = this.proxies.get(FOS_PROXY_DATETIME);
					if(this.datetime == null){
						// 時刻プロパティは未定義
						return;
					}
				}
				this.datetime.setCurrentValue(new Value(datetime.toString(), XSDDatatype.XSDdateTime));
			}
			
			// 監視している人に知らせる
			List<ValueConsumer> removed = null;
			for(ValueConsumer c: this.consumers){
				if(!c.informValueUpdate(this)){
					// 監視終了
					if(removed == null){
						removed = new ArrayList<>();
					}
					removed.add(c);
				}
			}
			
			// 監視終了したのがあればはずす
			if(removed != null){
				for(ValueConsumer c: removed){
					this.removeConsumer(c);
				}
			}
		}
		
		public void setValue(String predicate, Node node){
			if(predicate.equals(FOS_PROXY_DATETIME)){
				this.datetimeIncluded = true;
			}
			LeafProxy leaf = this.proxies.get(predicate);
			if(leaf != null){
				Value value = new Value(node.getLiteralValue().toString(), node.getLiteralDatatype(), node.getLiteralLanguage());  //??????
				//System.err.println(String.format("___%s___%s___%s___", node.getLiteralValue(), node.getLiteral().toString(), node.getLiteralLexicalForm()));
				//Object lvalue = node.getLiteralValue();
				//Value value = new Value(node.toString());
				leaf.setCurrentValue(value);
				System.err.println(String.format("VALUE:%s:%s:%s:%s:%s", uri, predicate, value.dtype, value.value, value.lang));
			}
			else {
				System.err.println(String.format("UNDEFINED LEAF:%s:%s:%s", uri, predicate, node.toString()));
			}
		}
		
		public Node getCurrentValue(String predicate) {
			LeafProxy proxy = this.proxies.get(predicate);
			if(proxy != null){
				return proxy.getCurrentValue();
			}
			else {
				return NodeFactory.createURI(FOS_PROXY_UNDEFINED);
			}
		}
		
		public Map<String,LeafProxy> getLeaves(){
			return this.proxies;
		}
		
		private String[] makePair(String s1, String s2){
			String[] ret = new String[2];
			ret[0] = s1;
			ret[1] = s2;
			return ret;
		}
		
		private String[] makeValuePair(String label, Node value){
			String[] ret = new String[2];
			ret[0] = label;
			if(value.isLiteral()){
				ret[1] = value.getLiteralLexicalForm();
			}
			else {
				ret[1] = value.toString();
				if(ret[1].equals(RealtimeValueBroker.FOS_PROXY_EMPTY)){
					ret[1] = "null";
				}
			}
			return ret;
		}
		
		public List<KVPair> getPropertyValuePairs(){
			List<KVPair>  ret = new ArrayList<>();
			ret.add(new KVPair(FOS_PROXY_ID, this.id));
			ret.add(new KVPair(FOS_PROXY_DATETIME_SHORT,this.proxies.get(FOS_PROXY_DATETIME_SHORT).getCurrentValue()));
			for(String shortName : this.propertyNames){
				ret.add(new KVPair(shortName,this.proxies.get(shortName).getCurrentValue()));
			}
			return ret;
		}
	}
}
