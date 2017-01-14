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

import java.util.HashMap;
import java.util.Map;

/**
 * @author m-nakagawa
 *
 */
public class FosNames {
	// TODO : Multi-language definition should be read from a file? 
    public static final String FOS_NAME_BASE = "http://bizar.aitc.jp/ns/fos/0.1/";
    
    public static final String FOS_CONFIG = FOS_NAME_BASE+"fos";
    //public static final String FOS_CONFIG_LOCALE = FOS_NAME_BASE+"locale";
    
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
	//public static final String FOS_PROXY_INSTANT_SHORT_ja = "時刻";
	//public static final String FOS_PROXY_INSTANT_SHORT_en = "instant";
	public static final String FOS_PROXY_INSTANT_SHORT = "instant";
	//public static final String FOS_PROXY_INSTANT_ja = FOS_NAME_BASE+FOS_PROXY_INSTANT_SHORT_ja;
	//public static final String FOS_PROXY_INSTANT_en = FOS_NAME_BASE+FOS_PROXY_INSTANT_SHORT_en;
	//public static final String FOS_PROXY_DATETIME_SHORT_ja = "日時";
	//public static final String FOS_PROXY_DATETIME_SHORT_en = "datetime";
	public static final String FOS_PROXY_DATETIME_SHORT = "datetime";
	//public static final String FOS_PROXY_DATETIME_ja = FOS_NAME_BASE+FOS_PROXY_DATETIME_SHORT_ja;
	//public static final String FOS_PROXY_DATETIME_en = FOS_NAME_BASE+FOS_PROXY_DATETIME_SHORT_en;
	
	public static final String FOS_PROXY_SYSTEM = FOS_PROXY_HUB+SYSTEM_ID;
	
	public static final String FOS_PATH_TAG = FOS_NAME_BASE+"tag";

	public static final String FOS_LEAFID_WEBSOCKET = SYSTEM_ID+"_websocket";
	public static final String FOS_SYSTEM_WEBSOCKET_CONNECTION = FOS_PROXY_LEAF+FOS_LEAFID_WEBSOCKET;
	public static final String FOS_WEBSOCKET_PREDICATE = "接続数";
	public static final String FOS_LEAFID_SEND = SYSTEM_ID+"_send";
	public static final String FOS_SYSTEM_WEBSOCKET_SEND = FOS_PROXY_LEAF+FOS_LEAFID_SEND;
	public static final String FOS_SEND_PREDICATE = "最大送信数";
	//public static final String FOS_DEFAULT_VALUE_TAG = "値";
	public static final String FOS_PROXY_ID = "id"; //Leafノードのidを返すときの値のラベル
	

	public static enum FosVocab {
		instant, datetime, pathTag;
		/*
		private static Map<FosVocab,Map<FosLocale,FosVocabItem>> vocabIndex = new HashMap<>();
		private static void setIndex(FosVocabItem item){
			Map<FosLocale,FosVocabItem> vmap = vocabIndex.get(item.vocab);
			if(vmap == null){
				vmap = new HashMap<>();
				FosVocab.vocabIndex.put(item.vocab, vmap);
			}
			vmap.put(item.locale, item);
		}
		
		public static Map<FosLocale,FosVocabItem> getLocaleIndex(FosVocab v){
			return FosVocab.vocabIndex.get(v);
		}
*/		
	}
/*
	public static enum FosLocale {
		ja, en;

		private static Map<FosLocale,Map<FosVocab,FosVocabItem>> localeIndex = new HashMap<>();
		private static void setIndex(FosVocabItem item){
			Map<FosVocab,FosVocabItem> lmap = localeIndex.get(item.locale);
			if(lmap == null){
				lmap = new HashMap<>();
				FosLocale.localeIndex.put(item.locale, lmap);
			}
			lmap.put(item.vocab, item);
		}
		
		public static Map<FosVocab,FosVocabItem> getVocabIndex(FosVocab v){
			return FosLocale.localeIndex.get(v);
		}
	}
*/
	public static class FosVocabItem {
		private final FosVocab vocab;   // 語彙（instant,datetimeなど）のID
		//private final FosLocale locale; // ロケール(ja,enなど）のID
		private final String shortLabel;// 語彙のロケールにおけるIRIのロケール依存部分
		private final String iri;       // 語彙のロケールにおけるIRI
		
		
		//private FosVocabItem(FosVocab vocab, FosLocale locale, String prefix, String shortLabel){
		private FosVocabItem(FosVocab vocab, String prefix, String shortLabel){
			this.vocab = vocab;
			//this.locale = locale;
			this.shortLabel = shortLabel;
			this.iri = prefix+shortLabel;
		}
		
		public FosVocab getVocab() {
			return vocab;
		}
		/*
		public FosLocale getLocale() {
			return locale;
		}
		*/
		public String getIri() {
			return iri;
		}
		public String getShortLabel() {
			return shortLabel;
		}
	};
	
	private static Map<String,FosVocabItem> iriIndex = new HashMap<>();
	
	private static void setIndex(FosVocabItem item){
		FosNames.iriIndex.put(item.iri, item);
		//FosLocale.setIndex(item);
		//FosVocab.setIndex(item);
	}
	
	static {
		setIndex(new FosVocabItem(FosVocab.instant,  FOS_NAME_BASE, FOS_PROXY_INSTANT_SHORT ));
		setIndex(new FosVocabItem(FosVocab.datetime, FOS_NAME_BASE, FOS_PROXY_DATETIME_SHORT ));
		/*
		setIndex(new FosVocabItem(FosVocab.instant, FosLocale.ja, FOS_NAME_BASE, FOS_PROXY_INSTANT_SHORT_ja ));
		setIndex(new FosVocabItem(FosVocab.instant, FosLocale.en, FOS_NAME_BASE, FOS_PROXY_INSTANT_SHORT_en ));
		setIndex(new FosVocabItem(FosVocab.datetime, FosLocale.ja, FOS_NAME_BASE, FOS_PROXY_DATETIME_SHORT_ja ));
		setIndex(new FosVocabItem(FosVocab.datetime, FosLocale.en, FOS_NAME_BASE, FOS_PROXY_DATETIME_SHORT_en ));
		*/
	}

	
	//private static FosLocale defaultLocale = null;
	
	/**
	 * @param iri
	 * @return nullなら未定義
	 */
	public static FosVocabItem getVocabItem(String iri){
		FosVocabItem ret = FosNames.iriIndex.get(iri);

		/*
		// ロケール未定義状態では、初めて使われたIRIのロケールをデフォルトロケールに設定する。
		if(FosNames.defaultLocale == null && ret != null){
			FosNames.defaultLocale = ret.getLocale();
		}
		*/
		return ret;
	}
	/*
	public static FosLocale getDefaultLocale(){
		if(FosNames.defaultLocale == null){
			// デフォルトロケールが設定されていなければ英語を返す。
			return FosLocale.en;
		}
		else {
			return FosNames.defaultLocale;
		}
	}
	*/
	
	/**
	 * 現在のデフォルトロケールのラベルを返す
	 * @param vocab
	 * @return
	 */
	public static String getShortLabel(FosVocab vocab){
		return vocab.toString();

		//FosVocabItem item = FosLocale.getVocabIndex(vocab).get(getDefaultLocale());
		//return item.getShortLabel();
	}
} 
