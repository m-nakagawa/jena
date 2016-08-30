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

import java.time.Instant;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.apache.jena.graph.Node_URI;
import org.slf4j.Logger;

/**
 * @author m-nakagawa
 *
 */
public class RealtimeValueBroker {
    private final static Logger log = getLogger(RealtimeValueBroker.class);

    public static final String FOS_NAME_BASE = "http://bizar.aitc.jp/ns/fos/0.1/";
	public static final String FOS_LOCAL_NAME_BASE = FOS_NAME_BASE+"local/";
	public static final String FOS_TAG_BASE = FOS_LOCAL_NAME_BASE+"label#";
	public static final String FOS_PROXY_BASE = FOS_LOCAL_NAME_BASE+"proxy/";
	//public static final String FOS_PROXY_EMPTY = FOS_PROXY_BASE+"empty";
	public static final String FOS_PROXY_UNDEFINED = FOS_PROXY_BASE+"undefined";
	public static final String FOS_PROXY_HOLDER = FOS_PROXY_BASE+"hub#";
	private static final String LEAF_ID = "leaf";
	private static final String ARRAY_ID = "array";
	public static final String FOS_PROXY_LEAF = FOS_PROXY_BASE+LEAF_ID+"#";
	public static final String FOS_PROXY_ARRAY = FOS_PROXY_BASE+ARRAY_ID+"#";
	public static final String FOS_PROXY_INSTANT_SHORT = "時刻";
	public static final String FOS_PROXY_DATETIME_SHORT = "日時";
	public static final String FOS_PROXY_INSTANT = FOS_NAME_BASE+FOS_PROXY_INSTANT_SHORT;
	public static final String FOS_PROXY_DATETIME = FOS_NAME_BASE+FOS_PROXY_DATETIME_SHORT;
	//public static final String FOS_DEFAULT_VALUE_TAG = "値";
	public static final String FOS_PROXY_ID = "id"; //Leafノードのidを返すときの値のラベル
	private static final Pattern fosProxyHolder = Pattern.compile(FOS_PROXY_HOLDER+"(.+)$"); 
	//private static final Pattern fosProxyLeaf = Pattern.compile(FOS_PROXY_LEAF+"(.+)$"); 
	private static final Pattern fosProxyLeafOrArray = Pattern.compile(FOS_PROXY_BASE+"("+LEAF_ID+"|"+ARRAY_ID+")#(.+)$"); 
	
	private static Map<String,LeafProxy> leafProxyIndex = new ConcurrentHashMap<>();
	private static Map<String,HubProxy> rootProxyIndex = new ConcurrentHashMap<>();
	private static Set<HubProxy> updatedProxies;
	private static Instant updateTime;
	private static ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();
	
	static {
		new TestProxy("http://bizar.aitc.jp/ns/fos/0.1/internal/01");
		// どれもデータ型を推測しない
		//LiteralLabel literal = LiteralLabelFactory.createLiteralLabel("123", "", (RDFDatatype)null);
		//LiteralLabel literal = LiteralLabelFactory.create("123", (RDFDatatype)null);
		//LiteralLabel literal = LiteralLabelFactory.createByValue("123", "", (RDFDatatype)null);
		//System.out.println(literal);
	}
	
	
	private static final Node[] NULL_VALUE_NODE = new Node[1];
	static {
		NULL_VALUE_NODE[0] = NodeFactory.createLiteralByValue("", "", XSDDatatype.XSDstring);
	}
	
	public interface LeafProxy {
		String getURI();
		void setCurrentValue(Value value, Instant instant);
		void setCurrentValues(Value[] values, Instant instant);
		Instant getUpdateInstant();
		Node[] getCurrentValue();
		boolean isArray();
	}
	

	public static class TestProxy implements LeafProxy {
		private final String uri;
		private int value;
		private Instant update;
		
		private TestProxy(String uri){
			this.uri = uri;
			this.value = 0;
			addLeafProxy(this);
		}
		
		public String getURI(){
			return this.uri;
		}
		
		public Instant getUpdateInstant(){
			return this.update;
		}
		
		public void setCurrentValue(Value value, Instant instant){
			this.update = instant;
			if(value.dtype == XSDDatatype.XSDint || value.dtype == XSDDatatype.XSDinteger){
				//this.value = Integer.valueOf(value.value);
				this.value = (Integer)value.value;
			}
			else {
				// 無視
			}
		}
		
		public void setCurrentValues(Value[] value, Instant instant){
		}
		
		public Node[] getCurrentValue(){
			++value;
			Node[] ret = new Node[1];
			ret[0] = NodeFactory.createLiteral(Integer.toString(value), XSDDatatype.XSDinteger);
			return ret;
		}
		
		public boolean isArray(){
			return false;
		}
	}

	/*
	private static int debugCnt = 0;
	public static class ReadContext {
		private ReadLock lock;
		int num;
		
		private ReadContext(ReadLock lock){
			this.num = debugCnt++;
			this.lock = lock;
		}
	}
	 */
	//private static Map<Object,ReadContext> locks = new HashMap<>();
	// 全Proxyを一時停止
	public static void freeze(Object owner){
		//System.err.println("FREEZE--------");
		//System.err.println("FREEZE--------"+Integer.toString(debugCnt));
		//ReadLock lock = readWriteLock.readLock();
		//lock.lock();
		//locks.put(owner, new ReadContext(lock));
	}
	
	public static void release(Object owner){
		//System.err.println("RELEASE--------");
		//ReadContext context = locks.get(owner);
		//context.lock.unlock();
	}
	

	public static class UpdateContext {
		private WriteLock lock;
		private Instant instant;
		
		private UpdateContext(WriteLock lock, Instant instant){
			this.lock = lock;
			this.instant = instant;
		}
		
		public Instant getInstant(){
			return this.instant;
		}
	}
	
	public static UpdateContext prepareUpdate(){
		System.err.println("PREPARE UPDATE--------");

		updateTime = Instant.now();

		WriteLock lock = readWriteLock.writeLock();
		lock.lock();

		UpdateContext ret = new UpdateContext(lock, updateTime);

		updatedProxies = new HashSet<>();
		return ret;
	}
	
	public static void finishUpdate(UpdateContext context){
		for(HubProxy proxy : updatedProxies){
			proxy.update(updateTime);
		}
		context.lock.unlock();
		System.err.println("FINISH UPDATE--------");
	}
	
	// return true: Proxyがデータを消費した false:新規登録した
	public static boolean isProxy(Node g, Node s, Node p, Node o){
		//System.err.println(String.format("XXX %s:%s:%s:%s", g.toString(), s.toString(), p.toString(), o.toString()));
		if(s instanceof Node_URI ){
			Matcher mHolder = fosProxyHolder.matcher(s.toString());
			if(mHolder.matches()){
				if(o instanceof Node_URI){
					Matcher mValue = fosProxyLeafOrArray.matcher(o.toString());
					if(mValue.matches()){
						boolean array = mValue.group(1).equals(ARRAY_ID);
						// 登録 
						HubProxy root = getRootProxy(s.toString());
						if(root == null){
							root = new HubProxy(s.toString());
							addRootProxy(root);
						}
						LeafProxy leaf = getLeafProxy(o.toString());
						if(leaf == null){
							leaf = new ValueContainer(o.toString(), array);
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
						root.setValue(p.toString(), o, updateTime);
						return true;
					}
				}
			}
		}
		return false;
	}
	
	

	public static class Value {
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

		public Value(Node node){
			this(node.getLiteralValue().toString(), node.getLiteralDatatype(), node.getLiteralLanguage());  //??????			
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
		private final boolean array;
		private Node[] value;
		private Instant instant; // 通常はHUBの方の時刻を使う。これは将来の拡張用
		
		private ValueContainer(String uri, boolean array){
			this.uri = uri;
			this.array = array;
			if(this.array){
				this.value = new Node[0];
			}
			else {
				this.value = NULL_VALUE_NODE;
			}
			this.instant = null;
			addLeafProxy(this);
		}
		
		@Override
		public String getURI(){
			return this.uri;
		}

		@Override
		public void setCurrentValue(Value value, Instant instant){
			this.instant = instant;
			this.value = new Node[1];
			this.value[0] = NodeFactory.createLiteralByValue(value.getValue(), value.getLang(), value.getDtype()); 
		}
		
		@Override
		public void setCurrentValues(Value[] values, Instant instant){
			this.instant = instant;
			this.value = new Node[values.length];
			for(int i = 0; i < values.length; ++i){
				this.value[i] = NodeFactory.createLiteralByValue(values[i].getValue(), values[i].getLang(), values[i].getDtype());
			}
		}
		
		@Override
		public Instant getUpdateInstant(){
			return this.instant;
		}
		
		@Override
		public Node[] getCurrentValue(){
			return this.value;
		}
		
		@Override
		public boolean isArray(){
			return this.array;
		}
	}
	
	public interface ValueConsumer {
		boolean informValueUpdate(HubProxy proxy); // return false: 登録を解除する
	}
	
	public final static class Pair<K,V> {
	    private final K key;
	    private final V value;

	    public Pair(K key, V value) {
	        this.key = key;
	        this.value = value;
	    }

	    public K getKey() {
	        return key;
	    }

	    public V getValue() {
	        return value;
	    }
	}
	

	public static class HubProxy {
		private final String uri;
		//private final Node id;
		private final String idStr;
		private final Map<String,LeafProxy> proxies;
		private final Set<ValueConsumer> consumers;
		private LeafProxy datetime; // 日時
		private LeafProxy instant; // 時刻
		private List<Pair<String,LeafProxy>> propertyNames;

		public HubProxy(String uri){
			this.uri = uri;
			this.proxies =  new ConcurrentHashMap<>();
			this.datetime = null;
			this.instant = null;
			this.consumers = new CopyOnWriteArraySet<>();
			this.propertyNames = new ArrayList<>();
			Matcher m = URI_SPLITTER.matcher(uri);
			if(m.matches()){
				this.idStr = m.group(2);
			}
			else {
				this.idStr = uri;
			}
			//this.id = NodeFactory.createLiteral(idStr, "", XSDDatatype.XSDstring);
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

		public String getIdStr(){
			return this.idStr;
		}
		
		private void makeShortnameIndex(){
			Map<String,List<LeafProxy>> map = new HashMap<>();
			Map<LeafProxy,String> reverse = new HashMap<>();
			for(Entry<String,LeafProxy> e : this.proxies.entrySet()){
				reverse.put(e.getValue(), e.getKey());
				String predicate = e.getKey();
				Matcher m = RealtimeValueBroker.URI_SPLITTER.matcher(predicate);
				String tag;
				if(m.matches() && m.group(2).length() != 0){
					tag = m.group(2);
				}
				else {
					tag = predicate;
				}
				List<LeafProxy> llf = map.get(tag);
				if(llf == null){
					llf = new ArrayList<>();
					map.put(tag, llf);
				}
				llf.add(e.getValue());
			}
			this.propertyNames = new ArrayList<>();
			for(Entry<String,List<LeafProxy>> e: map.entrySet()){
				List<LeafProxy> llf = e.getValue();
				if(llf.size() == 1){
					propertyNames.add(new Pair<String,LeafProxy>(e.getKey(), llf.get(0)));
				}
				else {
					// 短縮名が重複している
					for(LeafProxy lf: llf){
						// 短縮名ではなくフルのURIで
						propertyNames.add(new Pair<String,LeafProxy>(reverse.get(lf), lf));
					}
				}
			}
			propertyNames.sort((o1, o2) -> o2.getKey().compareTo(o1.getKey()));
		}
		
		public void addLeaf(String predicate, LeafProxy leaf){
			String leafUri = leaf.getURI();
			if(predicate.equals(FOS_PROXY_DATETIME)){
				this.datetime = leaf;
			}
			if(predicate.equals(FOS_PROXY_INSTANT)){
				this.instant = leaf;
			}
			
			if(this.proxies.containsKey(predicate)){
				// predicateが重複している
				log.error(String.format("Can't append %s to %s %s", leafUri, this.uri, predicate));
			}
			else {
				this.proxies.put(predicate, leaf);
			}
			makeShortnameIndex();
		}
		
		public void update(Instant instant){
			if(this.datetime != null){
				this.datetime.setCurrentValue(new Value(instant.toString(), XSDDatatype.XSDdateTime), instant);
			}
			if(this.instant != null){
				this.instant.setCurrentValue(new Value(new Long(instant.toEpochMilli()), XSDDatatype.XSDunsignedLong), instant);
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
		
		public void setValue(String predicate, Node node, Instant instant){
			LeafProxy leaf = this.proxies.get(predicate);
			if(leaf != null){
				//Value value = new Value(node.getLiteralValue().toString(), node.getLiteralDatatype(), node.getLiteralLanguage());  //??????
				Value value = new Value(node);
				//System.err.println(String.format("___%s___%s___%s___", node.getLiteralValue(), node.getLiteral().toString(), node.getLiteralLexicalForm()));
				//Object lvalue = node.getLiteralValue();
				//Value value = new Value(node.toString());
				leaf.setCurrentValue(value, instant);

				updatedProxies.add(this);
				//System.err.println(String.format("VALUE:%s:%s:%s:%s:%s", uri, predicate, value.dtype, value.value, value.lang));
			}
			else {
				System.err.println(String.format("UNDEFINED LEAF:%s:%s:%s", uri, predicate, node.toString()));
			}
		}
		
		public boolean setValues(String predicate, Value[] value, Instant instant){
			LeafProxy leaf = this.proxies.get(predicate);
			if(leaf != null){
				leaf.setCurrentValues(value, instant);
				updatedProxies.add(this);
				return true;
			}
			else {
				return false;
			}
		}
		
		/*
		public Node getCurrentValue(String predicate) {
			LeafProxy proxy = this.proxies.get(predicate);
			if(proxy != null){
				return proxy.getCurrentValue();
			}
			else {
				return NULL;
			}
		}
		*/
		
		public Map<String,LeafProxy> getLeaves(){
			return this.proxies;
		}
		
		public List<Pair<String,LeafProxy>> getPropertyValuePairs(){
			return this.propertyNames;
		}
	}
}
