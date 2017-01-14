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
import java.util.AbstractMap;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArraySet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.zip.DataFormatException;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.fosext.RealtimeValueBroker.UpdateContext;
import org.apache.jena.graph.Node;
import org.slf4j.Logger;


/**
 * @author m-nakagawa
 * ハブノード
 */
public class HubProxy {
    private final static Logger log = getLogger(RealtimeValueBroker.class);

    public static final Pattern URI_SPLITTER = Pattern.compile("^(.*[#|/])(.+)$");

    private Map<String,Integer> proxyIndex; 
    private List<Entry<String,LeafProxy>> properties;

    public class HubValue {
    	private Node[][] snapshot;
    	HubValue(){
    		int size = HubProxy.this.properties.size();
    		this.snapshot = new Node[size][];
    		for(int i = 0; i < size; ++i){
    			LeafProxy leaf = HubProxy.this.properties.get(i).getValue();
    			snapshot[i] = leaf.getCurrentValue();
    		}
    	}
    	
    	public Node[] getValue(String proxyName){
    		int index = HubProxy.this.proxyIndex.get(proxyName);
    		return snapshot[index];
    	}
    }
    
    private final String uri; // ノードのIRI
    private final String idStr; // ノードIRIの最後のパート（#または/から行末まで）

    private final Set<ValueConsumer> consumers;
    private LeafProxy datetime; // 日時
    private LeafProxy instant; // 時刻
    
    private List<Entry<String,LeafProxy>> propertyNames;


    private TimeSeries timeSeries; // 値の履歴を保持するオブジェクト
    private String formattedValue=null;
    private String jsonValue=null;
    private HubValue currentValue=null;

    /**
     * false: updateが呼ばれた時だけ値が変化する
     */
    private boolean volatileValue = false;

    public HubProxy(String uri){
    	this.uri = uri;
    	this.proxyIndex =  new ConcurrentHashMap<>();
    	this.properties = new ArrayList<>();
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

    	try {
    		this.timeSeries = new TimeSeries(uri);
    	} catch(DataFormatException e){
    		log.error("Can't register time series storage:"+e.toString());
    		this.timeSeries = null;
    	}
    }

    void setVolatile(){
    	this.volatileValue = true;
    }

    /**
     * 値変化の通知先を登録する。
     * @param consumer
     */
    public synchronized void addConsumer(ValueConsumer consumer){
    	this.consumers.add(consumer);
    }

    private synchronized void removeConsumer(ValueConsumer consumer){
    	this.consumers.remove(consumer);
    }

    /**
     * ノードIRIの取得
     * @return
     */
    public String getURI() {
    	return this.uri;
    }

    /**
     * ノードIRIの終端部（#か/の後）の取得
     * @return
     */
    public String getIdStr(){
    	return this.idStr;
    }

    private void makeShortnameIndex(){
    	Map<String,List<LeafProxy>> map = new HashMap<>();
    	Map<LeafProxy,String> reverse = new HashMap<>();
    	int size = this.properties.size();
    	for(int i = 0; i < size; ++i){
        	Entry<String,LeafProxy> e = this.properties.get(i);
        	LeafProxy leaf = e.getValue();
    		if(leaf == this.instant || leaf == this.datetime){
    			// 時刻だけ特別扱い
    			continue;
    		}
    		String predicate = e.getKey();
    		//System.out.println("!!!!!!!!!!!!!!x!"+predicate);
    		reverse.put(leaf, predicate);
    		Matcher m = URI_SPLITTER.matcher(predicate);
    		String tag;
    		if(m.matches() && m.group(2).length() != 0){
    			tag = m.group(2);
    		}
    		else {
    			tag = predicate;
    		}
    		//System.out.println("!!!!!!!!!!!!!!"+tag);
    		List<LeafProxy> llf = map.get(tag);
    		if(llf == null){
    			llf = new ArrayList<>();
    			map.put(tag, llf);
    		}
    		llf.add(leaf);
    	}
    	this.propertyNames = new ArrayList<>();
    	for(Entry<String,List<LeafProxy>> e: map.entrySet()){
    		List<LeafProxy> llf = e.getValue();
    		if(llf.size() == 1){
    			propertyNames.add(new AbstractMap.SimpleEntry<String,LeafProxy>(e.getKey(), llf.get(0)));
    		}
    		else {
    			// 短縮名が重複している
    			for(LeafProxy lf: llf){
    				// 短縮名ではなくフルのURIで
    				propertyNames.add(new AbstractMap.SimpleEntry<String,LeafProxy>(reverse.get(lf), lf));
    			}
    		}
    	}
    	propertyNames.sort((o1, o2) -> o2.getKey().compareTo(o1.getKey()));
    }

    /**
     * プロキシノードを設定する。
     * @param predicate
     * @param leaf
     */
    public synchronized void addLeaf(String predicate, LeafProxy leaf){
    	String leafUri = leaf.getURI();
    	FosNames.FosVocabItem item = FosNames.getVocabItem(predicate);
    	if(item != null){
    		// 予約語
    		switch(item.getVocab()){
    		case instant:
    			this.instant = leaf;
    			break;
    		case datetime:
    			this.datetime = leaf;
    			break;
    		default:
    			throw new RuntimeException("????");
    		}
    	}

    	if(this.proxyIndex.containsKey(predicate)){
    		// predicateが重複している
    		log.error(String.format("Can't append %s to %s %s", leafUri, this.uri, predicate));
    	}
    	else {
    		this.properties.add(new AbstractMap.SimpleEntry<String,LeafProxy>(predicate,leaf));
    		this.proxyIndex.put(predicate, this.properties.size()-1);
    	}
    	makeShortnameIndex();
    }

    /**
     * 値を更新する。
     * @param instant
     */
    void update(Instant instant){
    	if(this.datetime != null){
    		this.datetime.setCurrentValue(new LeafValue(instant.toString(), XSDDatatype.XSDdateTime), instant);
    	}
    	if(this.instant != null){
    		this.instant.setCurrentValue(new LeafValue(new Long(instant.toEpochMilli()), XSDDatatype.XSDunsignedLong), instant);
    	}

    	// ログをとる
    	this.currentValue = new HubValue(); // スナップショットを作成

    	this.format();
    	this.timeSeries.write(this.formattedValue);

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

    public TimeSeries getTimeSeries(){
    	return this.timeSeries;
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

    private static void expandValues(StringBuilder ret, LeafProxy proxy){
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

    public String getCuurentValueStr(){
    	if(this.formattedValue == null || this.volatileValue){
    		this.format();
    	}
    	return this.formattedValue;
    }

    public String toJSON(){
    	if(this.jsonValue == null || this.volatileValue){
    		StringBuilder ret = new StringBuilder();
    		ret.append('[');
    		ret.append(this.getIdInJSON());
    		ret.append(',');
    		ret.append(this.getCuurentValueStr());
    		ret.append(']');
    		this.jsonValue = ret.toString();
    	}
    	return this.jsonValue;
    }

    public String toString(){
    	return this.toJSON();
    }

    /**
     * ハブノードのIDをJSONのマップのエントリ文字列として返す。
     * "id":"m-sdfdsf"
     * @return
     */
    public String getIdInJSON(){
    	StringBuilder ret = new StringBuilder();
    	//ret.append("\""+FosNames.FOS_PROXY_ID+"\":\"");
    	ret.append('"');
    	ret.append(escape(this.getIdStr()));
    	ret.append('"');
    	return ret.toString();
    }

    private void format(){
    	this.jsonValue = null;
    	List<Entry<String,LeafProxy>> values = this.getPropertyValuePairs();
    	StringBuilder ret = new StringBuilder();
    	ret.append('{');
    	if(this.instant != null){
    		ret.append('"');
    		ret.append(FosNames.getShortLabel(FosNames.FosVocab.instant));
    		ret.append("\":");
    		expandValues(ret, this.instant);
    		ret.append(',');
    	}
    	if(this.datetime != null){
    		ret.append('"');
    		ret.append(FosNames.getShortLabel(FosNames.FosVocab.datetime));
    		ret.append("\":");
    		expandValues(ret, this.datetime);
    		ret.append(',');
    	}
    	if(values.size() != 0){
    	for(int i = 0;;){
    		Entry<String,LeafProxy> p = values.get(i);
    		ret.append('"');
    		ret.append(p.getKey());
    		ret.append("\":");
    		expandValues(ret, p.getValue());
    		if(++i == values.size()){
    			break;
    		}
    		ret.append(',');
    	}
    	}
    	ret.append('}');
    	this.formattedValue = ret.toString();
    }

    public void setValue(String predicate, Node node, Instant instant){
    	LeafProxy leaf = this.properties.get(this.proxyIndex.get(predicate)).getValue();
    	if(leaf != null){
    		LeafValue value = new LeafValue(node);
    		//System.err.println(String.format("HubProxy.setValue:___%s___%s___%s___", node.getLiteralValue(), node.getLiteral().toString(), node.getLiteralLexicalForm()));
    		leaf.setCurrentValue(value, instant);

    		RealtimeValueBroker.informUpdate(this);
    		//System.err.println(String.format("VALUE:%s:%s:%s:%s:%s", uri, predicate, value.dtype, value.value, value.lang));
    	}
    	else {
    		System.err.println(String.format("UNDEFINED LEAF:%s:%s:%s", uri, predicate, node.toString()));
    	}
    }

    public boolean setValues(String predicate, LeafValue[] value, UpdateContext context){
    	Instant instant = context.getInstant();
    	Integer pos = this.proxyIndex.get(predicate);
    	if(pos == null){
    		return false;
    	}
    	LeafProxy leaf = this.properties.get(pos).getValue();
    	if(leaf != null){
    		leaf.setCurrentValues(value, instant);
    		RealtimeValueBroker.informUpdate(this);
    		return true;
    	}
    	else {
    		return false;
    	}
    }

    public HubValue getCurrentValue(){
    	return this.currentValue;
    }


    public List<Entry<String,LeafProxy>> getPropertyValuePairs(){
    	return this.propertyNames;
    }
}
