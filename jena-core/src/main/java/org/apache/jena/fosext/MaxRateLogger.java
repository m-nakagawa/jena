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
 *　秒間の事象発生数をカウントし、最大値を記録する。
 */
public class MaxRateLogger implements LeafProxy {

	private final String uri;
	private final AtomicInteger value;
	private long max;
	//private long current;
	private Instant update;
	private Thread th;

	MaxRateLogger(String uri){
		this.uri = uri;
		this.value = new AtomicInteger(0);
		this.max = 0;
		//this.current = 0;
		RealtimeValueBroker.addLeafProxy(this);

		this.th = new Thread(() -> {
			long prev = value.get();
			long prevt = Instant.now().toEpochMilli();
			for(;;){
				try{
					Thread.sleep(1000); // 1秒
				}catch (InterruptedException e){
				}
				int next = value.get();
				long nextt = Instant.now().toEpochMilli();
				long intrvl = next - prev;
				long intrvlt = nextt-prevt;
				if(intrvlt > 500){
					intrvl = (intrvl*1000)/intrvlt;
					//this.current = intrvl;
					if(this.max < intrvl){
						max = intrvl;
						RealtimeValueBroker.getSystemHub().update(Instant.now());
					}
				}
				prev = next;
				prevt = nextt;
			}
		});

		this.th.start();
	}

	public String getURI(){
		return this.uri;
	}

	public Instant getUpdateInstant(){
		return this.update;
	}

	public void setCurrentValue(LeafValue value, Instant instant){
		this.max = 0;
		RealtimeValueBroker.getSystemHub().update(Instant.now());
	}

	public void setCurrentValues(LeafValue[] value, Instant instant){
		this.max = 0;
		RealtimeValueBroker.getSystemHub().update(Instant.now());
	}

	public int inc(){
		int ret = this.value.incrementAndGet();
		return ret;
	}

	public int dec(){
		int ret = this.value.decrementAndGet();
		return ret;
	}

	public Node[] getCurrentValue(){
		Node[] ret = new Node[1];
		ret[0] = NodeFactory.createLiteral(Long.toString(this.max), XSDDatatype.XSDinteger);
		return ret;
	}

	public boolean isArray(){
		return false;
	}

}
