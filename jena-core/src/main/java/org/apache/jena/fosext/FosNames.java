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

/**
 * @author m-nakagawa
 *
 */
public class FosNames {
    public static final String FOS_NAME_BASE = "http://bizar.aitc.jp/ns/fos/0.1/";
    
	public static final String FOS_LOCAL_NAME_BASE = FOS_NAME_BASE+"local/";
	public static final String FOS_TAG_BASE = FOS_LOCAL_NAME_BASE+"label#";
	public static final String FOS_PROXY_BASE = FOS_LOCAL_NAME_BASE+"proxy/";
	public static final String FOS_SERVICE_BASE = FOS_LOCAL_NAME_BASE+"service/";
	public static final String FOS_SERVICE_UNIQUE = FOS_SERVICE_BASE+"uniqueNumber";
	//public static final String FOS_PROXY_EMPTY = FOS_PROXY_BASE+"empty";
	public static final String FOS_PROXY_UNDEFINED = FOS_PROXY_BASE+"undefined";
	public static final String FOS_PROXY_HUB = FOS_PROXY_BASE+"hub#";
	public static final String LEAF_ID = "leaf";
	public static final String ARRAY_ID = "array";
	public static final String SYSTEM_ID = "_system";
	public static final String FOS_PROXY_LEAF = FOS_PROXY_BASE+LEAF_ID+"#";
	public static final String FOS_PROXY_ARRAY = FOS_PROXY_BASE+ARRAY_ID+"#";
	public static final String FOS_PROXY_INSTANT_SHORT = "時刻";
	public static final String FOS_PROXY_DATETIME_SHORT = "日時";
	public static final String FOS_PROXY_INSTANT = FOS_NAME_BASE+FOS_PROXY_INSTANT_SHORT;
	public static final String FOS_PROXY_DATETIME = FOS_NAME_BASE+FOS_PROXY_DATETIME_SHORT;
	
	public static final String FOS_PROXY_SYSTEM = FOS_PROXY_HUB+SYSTEM_ID;
	
	public static final String FOS_LEAFID_WEBSOCKET = SYSTEM_ID+"_websocket";
	public static final String FOS_SYSTEM_WEBSOCKET_CONNECTION = FOS_PROXY_LEAF+FOS_LEAFID_WEBSOCKET;
	public static final String FOS_WEBSOCKET_PREDICATE = "接続数";
	public static final String FOS_LEAFID_SEND = SYSTEM_ID+"_send";
	public static final String FOS_SYSTEM_WEBSOCKET_SEND = FOS_PROXY_LEAF+FOS_LEAFID_SEND;
	public static final String FOS_SEND_PREDICATE = "最大送信数";
	//public static final String FOS_DEFAULT_VALUE_TAG = "値";
	public static final String FOS_PROXY_ID = "id"; //Leafノードのidを返すときの値のラベル
}
