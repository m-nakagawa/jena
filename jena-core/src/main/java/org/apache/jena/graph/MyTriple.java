/*
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
/**
 * 
 */
package org.apache.jena.graph;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;

/**
 * MNakagawa
 * @author m-nakagawa
 * 
 */
public class MyTriple extends Triple {
	public static final String FOS_PROXY_EMPTY = "http://bizar.aitc.jp/ns/fos/0.1/local/proxy/empty";
	public static final String FOS_PROXY_UNDEFINED = "http://bizar.aitc.jp/ns/fos/0.1/local/proxy/undefined";
	public static final String FOS_PROXY_DATETIME = "http://bizar.aitc.jp/ns/fos/0.1/時刻";
	public static final String FOS_PROXY_HOLDER = "http://bizar.aitc.jp/ns/fos/0.1/local/proxy/holder#";
	public static final String FOS_PROXY_VALUE = "http://bizar.aitc.jp/ns/fos/0.1/local/proxy/value#";
	private static final Pattern fosProxyHolder = Pattern.compile(FOS_PROXY_HOLDER+"(.*)$"); 
	private static final Pattern fosProxyValue = Pattern.compile(FOS_PROXY_VALUE+"(.*)$"); 
	
	static class Value {
		private final String value;
		private final String lang;
		private final RDFDatatype dtype;
		public Value(String value, RDFDatatype dtype, String lang){
			this.value = value;
			this.lang = lang;
			this.dtype = dtype;
			//this.dtype = null;
		}
		
		public Value(String value){
			this(value, XSDDatatype.XSDstring, null);
		}

		public Value(String value, RDFDatatype dtype){
			this(value, dtype, null);
		}

		public String getValue() {
			return value;
		}

		public String getLang() {
			return lang;
		}

		public RDFDatatype getDtype() {
			return dtype;
		}
	}
	
	interface LeafProxy {
		String getURI();
		void setCurrentValue(Value value);
		Node getCurrentValue();
	}

	private static Map<String,LeafProxy> leafProxyIndex = new HashMap<>();
	private static Map<String,RootProxy> rootProxyIndex = new HashMap<>();

	static void addLeafProxy(LeafProxy proxy){
		leafProxyIndex.put(proxy.getURI(), proxy);
	}
	
	static LeafProxy getLeafProxy(String name){
		return leafProxyIndex.get(name);
	}
	
	static void addRootProxy(RootProxy proxy){
		rootProxyIndex.put(proxy.getURI(), proxy);
	}
	
	public static RootProxy getRootProxy(String name){
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
				return NodeFactory.createLiteral(value.getValue(), value.getLang(), value.getDtype());
			}
			else {
				return NodeFactory.createURI(FOS_PROXY_EMPTY);
			}
		}
	}
	
	public static class RootProxy {
		private final String uri;
		private final Map<String,LeafProxy> proxies;
		private boolean datetimeIncluded = false; // true:setValueで日付時刻が設定された。 この値はsetDateTimeでfaluseに戻る。
		private LeafProxy datetime;

		public RootProxy(String uri){
			this.uri = uri;
			this.proxies =  new HashMap<>();
			this.datetime = null;
		}

		public String getURI() {
			return this.uri;
		}

		public void addLeaf(String predicate, LeafProxy leaf){
			this.proxies.put(predicate, leaf);
		}
		
		public void setDateTime(LocalDateTime datetime){
			if(datetimeIncluded){
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
		}
		
		public void setValue(String predicate, Node node){
			if(predicate.equals(FOS_PROXY_DATETIME)){
				this.datetimeIncluded = true;
			}
			LeafProxy leaf = this.proxies.get(predicate);
			if(leaf != null){
				Value value = new Value(node.toString(), node.getLiteralDatatype(), node.getLiteralLanguage());
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
				this.value = Integer.valueOf(value.value);
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


	static {
		new TestProxy("http://bizar.aitc.jp/ns/fos/0.1/internal/01");
	}
	

	// ここにあるのは変だがとりあえず
	// 全Proxyを一時停止
	public static void freeze(){
		System.out.println("FREEZE--------");
	}
	
	public static void release(){
		System.out.println("RELEASE--------");
	}
	
	private static Set<RootProxy> updatedProxies;
	private static LocalDateTime updateTime;

	public static void prepareUpdate(){
		updateTime = LocalDateTime.now();
		updatedProxies = new HashSet<>();
		System.out.println("PREPARE UPDATE--------");
	}
	
	public static void finishUpdate(){
		System.out.println("FINISH UPDATE--------");
		for(RootProxy proxy : updatedProxies){
			proxy.setDateTime(updateTime);
		}
	}
	
	// return true: Proxyがデータを消費した
	public static boolean isProxy(Node g, Node s, Node p, Node o){
		System.err.println(String.format("%s:%s:%s:%s", g.toString(), s.toString(), p.toString(), o.toString()));
		if(s instanceof Node_URI ){
			Matcher mHolder = fosProxyHolder.matcher(s.toString());
			if(mHolder.matches()){
				if(o instanceof Node_URI){
					Matcher mValue = fosProxyValue.matcher(o.toString());
					if(mValue.matches()){
						// 登録 
						RootProxy root = getRootProxy(s.toString());
						if(root == null){
							root = new RootProxy(s.toString());
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
					RootProxy root = getRootProxy(s.toString());
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
	
	private static Node proxy2value(Node node){
		if(node instanceof Node_URI){
			LeafProxy proxy = leafProxyIndex.get(node.getURI());
			if(proxy != null){
				return proxy.getCurrentValue();
			}
		}
		return node;
	}
	
	public MyTriple( Node s, Node p, Node o ){
		super(
				proxy2value(s),
				proxy2value(p),
				proxy2value(o)); //!MNakagawa oだけか？
	}
}
