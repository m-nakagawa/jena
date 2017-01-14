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
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.concurrent.locks.ReentrantReadWriteLock.WriteLock;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_URI;
import org.slf4j.Logger;

/**
 * @author m-nakagawa
 * <ul>
 * <li>値の書き込み
 * <ul>
 * <li>prepareUpdate
 * <li>JenaからはisProxyValue（ここからはHubProxy.setValue）で、PUTとWebSocketからはHubProxy.setValues
 * <ul>
 * <li>WebSocketへ直ちに値を送信
 * <li>timeSeriesへ値追加
 * </ul>
 * <li>finishUpdate
 * </ul>
 * <li>Jenaからの値の読出し<br>freeze直前の値をtimeSeriesから返す。
 * <ul>
 * <li>freezeする<br>ここでロックを取得し、他の検索とtimeSeriesによる書き出しをとめる。
 * <li>proxy2valueで読み出す
 * <li>releaseする
 * </ul>
 * </ul>
 */
public class RealtimeValueBroker {
	private final static Logger log = getLogger(RealtimeValueBroker.class);

	private static final Pattern fosProxyHolder = Pattern.compile(FosNames.FOS_PROXY_HUB+"(.+)$"); 
	private static final Pattern fosProxyLeafOrArray = Pattern.compile(FosNames.FOS_PROXY_BASE+"("+FosNames.LEAF_ID+"|"+FosNames.ARRAY_ID+")#(.+)$"); 
	
	private static Map<String,LeafProxy> leafProxyIndex = new ConcurrentHashMap<>();
	private static Map<String,HubProxy> rootProxyIndex = new ConcurrentHashMap<>();
	private static Set<HubProxy> updatedProxies;
	private static Instant updateTime;
	// TODO ロックフリーにする
	private static ReentrantReadWriteLock readWriteLock = new ReentrantReadWriteLock();

	static void informUpdate(HubProxy hub){
		updatedProxies.add(hub);
	}
	
	private static HubProxy getSystemProxy(String name){
		HubProxy hub = getHubProxy(name);
		if(hub == null){
			hub = new HubProxy(name);
			addRootProxy(hub);
		}
		return hub;
	}
	
	private static CountLogger websocketCnt ;
	private static MaxRateLogger sendCnt;
	private static HubProxy systemHub;
	
	static {
		websocketCnt = new CountLogger(FosNames.FOS_SYSTEM_WEBSOCKET_CONNECTION);
		sendCnt = new MaxRateLogger(FosNames.FOS_SYSTEM_WEBSOCKET_SEND);
		// どれもデータ型を推測しない
		//LiteralLabel literal = LiteralLabelFactory.createLiteralLabel("123", "", (RDFDatatype)null);
		//LiteralLabel literal = LiteralLabelFactory.create("123", (RDFDatatype)null);
		//LiteralLabel literal = LiteralLabelFactory.createByValue("123", "", (RDFDatatype)null);
		//System.out.println(literal);
		systemHub = getSystemProxy(FosNames.FOS_PROXY_SYSTEM);
		systemHub.addLeaf(FosNames.FOS_NAME_BASE+FosNames.FOS_WEBSOCKET_PREDICATE, websocketCnt);
		systemHub.addLeaf(FosNames.FOS_NAME_BASE+FosNames.FOS_SEND_PREDICATE, sendCnt);
		systemHub.setVolatile();
	}
	
	/**
	 * 現在接続せしているWebSocketの個数を返す。
	 * @return 
	 */
	public static CountLogger getWebsocketCnt(){
		return websocketCnt;
	}
	
	/**
	 * これまでの秒間送信数の最大値を返す。
	 * @return
	 */
	public static MaxRateLogger getSendCnt(){
		return sendCnt;
	}


	public static HubProxy getSystemHub(){
		return systemHub;
	}
	

	
	private static ThreadLocal<Map<Node,HubProxy.HubValue>> node2hub  = new ThreadLocal<Map<Node,HubProxy.HubValue>>(){
		public Map<Node,HubProxy.HubValue> initialValue(){
			return null;
		}
	};

	

	/**
	 * Sparqlクエリのために値更新を凍結する。
	 * Jenaの構造がよくわかっていなが、1個のクエリの複数スレッドによる並列処理は実行していなさそうであるため、freezeからreleaseまでスレッドローカル変数でコンテキストを保持する。
	 * これにより複数クエリをそれぞれ別スレッドで実行することが可能である。本当は、クエリのコンテキストをproxy2valueで参照できることがのぞましい。
	 * <p>freeze->[proxy2value,...]->release
	 * @param owner
	 */
	public static void freeze(Object owner){
		// 後で外す
		//operationThread.put(Thread.currentThread(),1);

		log.trace("FREEZE--------");
		node2hub.set(new HashMap<>());
	}
	
	/**
	 * Sparqlクエリのための値更新を再開する
	 * @param owner
	 */
	public static void release(Object owner){
		// 後で外す
		//if(!operationThread.containsKey(Thread.currentThread())){
		//	throw new RuntimeException("Different Thread!!!");
		//}
		
		//operationThread.remove(Thread.currentThread());
		
		node2hub.set(null);
		log.trace("RELEASE--------");
	}
	

	public static class UpdateContext {
		private boolean exclusive;
		private WriteLock lock;
		private Instant instant;
		
		private UpdateContext(boolean exclusive, WriteLock lock, Instant instant){
			this.exclusive = exclusive;
			this.lock = lock;
			this.instant = instant;
		}
		
		public Instant getInstant(){
			return this.instant;
		}
		
		public boolean isExclusive(){
			return this.exclusive;
		}
	}
	
	/**
	 * 値更新を開始する。
	 * <p>
	 * prepareUpdateとfinishUpdateの間につぎのいずれかの方法で値を更新する。
	 * <ul>
	 * <li>exclusive == true: isProxyでアップデートする
	 * <li>exclusive == false: HubProxy.setValuesでアップデートする
	 * </ul>
	 * @return
	 */
	public static UpdateContext prepareUpdate(boolean exclusive){
		//System.err.println("PREPARE UPDATE--------");
		log.trace("PREPARE UPDATE--------");
		node2hub.set(new HashMap<>());
		updateTime = Instant.now();
		
		WriteLock lock = readWriteLock.writeLock();
		lock.lock();

		UpdateContext ret = new UpdateContext(exclusive, lock, updateTime);

		updatedProxies = new HashSet<>();
		return ret;
	}
	
	/**
	 * prepareUpdateで開始した値更新を完結する。
	 * @param context
	 */
	public static void finishUpdate(UpdateContext context){
		for(HubProxy proxy : updatedProxies){
			proxy.update(updateTime);
		}
		node2hub.set(null);
		context.lock.unlock();
		//System.err.println("FINISH UPDATE--------");
		log.trace("FINISH UPDATE--------");
	}
	
	/**
	 * 物理ノードの定義や値設定であれば処理する。Jena本体からの呼び出し
	 * <ul>
	 * <li>sがハブでoが値であれば値を設定してtrueを返す。
	 * <li>sがハブでoがプロキシであればデータ構造を登録してfalseを返す。
	 * <li>上記以外はなにもせずにfalseを返す。
	 * </ul>
	 * 
	 * @param g
	 * @param s
	 * @param p
	 * @param o
	 * @return
	 */
	public static boolean isProxy(Node g, Node s, Node p, Node o){
		//System.err.println(String.format("isProxy %s:%s:%s:%s", g.toString(), s.toString(), p.toString(), o.toString()));
		if(s instanceof Node_URI ){
			String iri = s.toString();
			Matcher mHolder = fosProxyHolder.matcher(iri);
			if(mHolder.matches()){
				if(o instanceof Node_URI){
					Matcher mValue = fosProxyLeafOrArray.matcher(o.toString());
					if(mValue.matches()){
						boolean array = mValue.group(1).equals(FosNames.ARRAY_ID);
						// 登録 
						HubProxy hub = getHubProxy(s.toString());
						if(hub == null){
							hub = new HubProxy(s.toString());
							addRootProxy(hub);
						}
						LeafProxy leaf = getLeafProxy(o.toString());
						if(leaf == null){
							leaf = new LeafProxyImpl(o.toString(), array);
						}
						hub.addLeaf(p.toString(), leaf);
						//System.err.println(String.format("REGISTERED:%s:%s:%s", s.toString(), p.toString(), o.toString()));
						log.debug(String.format("REGISTERED:%s:%s:%s", s.toString(), p.toString(), o.toString()));
						return false;
					}
				}
				else {
					// 値の設定
					HubProxy root = getHubProxy(s.toString());
					if(root != null){
						root.setValue(p.toString(), o, updateTime);
						return true;
					}
				}
			}
			/*
			else if(iri.equals(FosNames.FOS_CONFIG)){
			}
			*/
		}
		return false;
	}
	
	/**
	 * 物理ノードの定義や値設定であれば処理する。Jena本体からのよびだし
	 * <ul>
	 * <li>sがハブでoがプロキシであればデータ構造を削除してtrueを返す。
	 * <li>上記以外はなにもせずにfalseを返す。
	 * </ul>
	 * 
	 * @param g
	 * @param s
	 * @param p
	 * @param o
	 * @return
	 */
	/*
	public static boolean removeProxy(Node g, Node s, Node p, Node o){
		// Not implemented
		return false;
	}
	*/	

	static void addLeafProxy(LeafProxy proxy){
		leafProxyIndex.put(proxy.getURI(), proxy);
	}
	
	/**
	 * IRIでプロキシノードを取得する。
	 * @param name
	 * @return
	 */
	public static LeafProxy getLeafProxy(String name){
		return leafProxyIndex.get(name);
	}
	
	static void addRootProxy(HubProxy proxy){
		rootProxyIndex.put(proxy.getURI(), proxy);
	}
	
	
	/**
	 * IRIでハブノードを取得する。
	 * @param name
	 * @return
	 */
	public static HubProxy getHubProxy(String name){
		return rootProxyIndex.get(name);
	}

	

	/**
	 * 物理ノードの現在値を取得する。jena本体からの読出しで使う。
	 * @param node
	 * @return
	 */
	public static List<Node> proxy2value(Node subject, Node predicate, Node object){
		// 後ではずす
		//if(!operationThread.containsKey(Thread.currentThread())){
		//	throw new RuntimeException("Different Thread!!!");
		//}
		
		if(subject instanceof Node_URI && predicate instanceof Node_URI){
			Map<Node,HubProxy.HubValue> hubIndex = node2hub.get();
			if(hubIndex == null){
				return null;
			}
			HubProxy.HubValue hubValue = hubIndex.get(subject);
			if(hubValue == null){
				String uri = subject.getURI();
				HubProxy hub = getHubProxy(uri);
				if(hub == null){
					return null;
				}
				hubValue = hub.getCurrentValue();
				if(hubValue == null){
					return null;
				}
				hubIndex.put(subject, hubValue);
			}
			Node[] ret = hubValue.getValue(predicate.getURI());
			if(ret != null){
				return Arrays.asList(ret);
			}
			return null;
		}
		return null;
	}
}
