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

import java.time.Instant;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;
import org.apache.jena.graph.NodeFactory;

/**
 * @author m-nakagawa
 *　カウント値を保持するクラス。
 */
public class CountLogger implements LeafProxy {

	private final String uri;
	private AtomicInteger value;
	private Instant update;

	CountLogger(String uri){
		this.uri = uri;
		this.value = new AtomicInteger(0);
		RealtimeValueBroker.addLeafProxy(this);
	}

	public String getURI(){
		return this.uri;
	}

	public Instant getUpdateInstant(){
		return this.update;
	}

	public void setCurrentValue(LeafValue value, Instant instant){
		assert(false);
	}

	public void setCurrentValues(LeafValue[] value, Instant instant){
	}

	public int inc(){
		int ret = this.value.incrementAndGet();
		RealtimeValueBroker.getSystemHub().update(Instant.now()); //TODO 時刻とトランザクションにするか
		return ret;
	}

	public int dec(){
		int ret = this.value.decrementAndGet();
		RealtimeValueBroker.getSystemHub().update(Instant.now()); //TODO 時刻とトランザクションにするか
		return ret;
	}

	public Node[] getCurrentValue(){
		Node[] ret = new Node[1];
		ret[0] = NodeFactory.createLiteral(Integer.toString(value.get()), XSDDatatype.XSDinteger);
		return ret;
	}

	public boolean isArray(){
		return false;
	}

}
