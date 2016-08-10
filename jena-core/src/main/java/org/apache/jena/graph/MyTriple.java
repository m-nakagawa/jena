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
import java.util.Map;

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;

/**
 * MNakagawa
 * @author m-nakagawa
 * 
 */
public class MyTriple extends Triple {
	public static class Value {
		private final String value;
		private final String lang;
		private final RDFDatatype dtype;
		private final LocalDateTime datetime;
		public Value(LocalDateTime datetime, String value, RDFDatatype dtype, String lang){
			this.datetime = datetime;
			this.value = value;
			this.lang = lang;
			this.dtype = dtype;
		}
		
		public Value(LocalDateTime datetime, String value){
			this(datetime, value, XSDDatatype.XSDstring, null);
		}

		public Value(LocalDateTime datetime, String value, RDFDatatype dtype){
			this(datetime, value, dtype, null);
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

		public LocalDateTime getDatetime() {
			return datetime;
		}
	}
	
	interface Proxy {
		String getURI();
		Node getCurrentValue();
	}

	interface ValueSupplier {
		public Value getValue();
	}
	
	private static Map<String,Proxy> proxyIndex = new HashMap<>();
	
	public static void addproxy(Proxy proxy){
		proxyIndex.put(proxy.getURI(), proxy);
	}

	public static class Scalarproxy implements Proxy {
		private String uri;
		private ValueSupplier proxy;

		public Scalarproxy(String uri, ValueSupplier proxy){
			this.uri = uri;
			this.proxy = proxy;
		}

		@Override
		public String getURI() {
			return this.uri;
		}
		
		public ValueSupplier getProxy(){
			return this.proxy;
		}

		@Override
		public Node getCurrentValue() {
			Value value = proxy.getValue();
			return NodeFactory.createLiteral(value.getValue(), value.getLang(), value.getDtype());
		}
		
	}

	static class TestProxy implements ValueSupplier {
		int cnt = 0;
		@Override
		public Value getValue() {
			++cnt;
			Value ret = new Value(LocalDateTime.now(), Integer.toString(cnt), XSDDatatype.XSDint);
			return ret;
		}
	}

	static {
		addproxy(new Scalarproxy("http://bizar.aitc.jp/ns/fos/0.1/internal/01", new TestProxy()));		
	}
	

	// ここにあるのは変だがとりあえず
	// 全Proxyを一時停止
	public static void freeze(){
		System.out.println("FREEZE--------");
	}
	
	public static void release(){
		System.out.println("RELEASE--------");
	}
	
	private static Node proxy2value(Node node){
		if(node instanceof Node_URI){
			Proxy proxy = proxyIndex.get(node.getURI());
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
				proxy2value(o));
	}
}
