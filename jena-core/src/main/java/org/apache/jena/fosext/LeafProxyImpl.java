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
import java.util.concurrent.atomic.AtomicLong;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;
import org.slf4j.Logger;

/**
 * @author m-nakagawa
 *
 */
public class LeafProxyImpl implements LeafProxy {
    private final static Logger log = getLogger(LeafProxy.class);

    private static AtomicLong generation = new AtomicLong(0); // システムの状態番号 
	private static final Node[] NULL_VALUE_NODE = new Node[1];
	static {
		NULL_VALUE_NODE[0] = NodeFactory.createLiteralByValue("", "", XSDDatatype.XSDstring);
	}

	private final String uri;
	private final boolean array;
	private Node[] value;
	private Instant instant; // 通常はHUBの方の時刻を使う。これは将来の拡張用

	public static long getGeneration(){
		return generation.incrementAndGet();
	}
	
	public static long getLock(){
		return 0;
	}
	
	LeafProxyImpl(String uri, boolean array) {
		this.uri = uri;
		this.array = array;
		if(this.array){
			this.value = new Node[0];
		}
		else {
			this.value = NULL_VALUE_NODE;
		}
		this.instant = null;
		
		RealtimeValueBroker.addLeafProxy(this);
		log.debug("ValueContainer registered:"+uri);
	}

	@Override
	public String getURI(){
		return this.uri;
	}

	@Override
	public void setCurrentValue(LeafValue value, Instant instant){
		this.instant = instant;
		this.value = new Node[1];
		this.value[0] = NodeFactory.createLiteralByValue(value.getValue(), value.getLang(), value.getDtype()); 
	}

	@Override
	public void setCurrentValues(LeafValue[] values, Instant instant){
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