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

import org.apache.jena.datatypes.RDFDatatype;
import org.apache.jena.datatypes.xsd.XSDDatatype;
import org.apache.jena.graph.Node;

/**
 * @author m-nakagawa
 *　プロキシが保持する値のクラス
 * TODO 値の型の扱いを再検討する必要があるだろう。
 */
public class LeafValue {

	private final Object value;
	private final String lang;
	private final RDFDatatype dtype;

	LeafValue(Object value, RDFDatatype dtype, String lang){
		this.value = value;
		this.lang = lang;
		this.dtype = dtype;
		//this.dtype = null;
	}

	public LeafValue(Object value){
		this(value, XSDDatatype.XSDstring, null);
	}

	public LeafValue(Object value, RDFDatatype dtype){
		this(value, dtype, null);
	}

	public LeafValue(Node node){
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
