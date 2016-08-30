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
package org.apache.jena.fosext;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.jena.graph.Node;
import org.apache.jena.graph.Node_URI;
import org.apache.jena.graph.Triple;

/**
 * MNakagawa
 * @author m-nakagawa
 * 
 */
//public class MyTriple extends Triple {
public class MyTriple {
	public static List<Node> proxy2value(Node node){
		if(node instanceof Node_URI){
			RealtimeValueBroker.LeafProxy proxy = RealtimeValueBroker.getLeafProxy(node.getURI());
			if(proxy != null){
				Node[] ret = proxy.getCurrentValue();
				return Arrays.asList(ret);
			}
		}
		return null;
	}
/*	
	private static Node proxy2singleValue(Node node){
		if(node instanceof Node_URI){
			RealtimeValueBroker.LeafProxy proxy = RealtimeValueBroker.getLeafProxy(node.getURI());
			if(proxy != null){
				return proxy.getCurrentValue();
			}
		}
		return node;
	}

	public MyTriple( Node s, Node p, Node o ){
		super(
				s,
				p,
				proxy2singleValue(o)); //!MNakagawa oだけか？
	}
	*/
}
