package org.apache.jena.fosext;

/**
 * @author m-nakagawa
 *　値変化の通知先オブジェクト
 */
public interface ValueConsumer {

	boolean informValueUpdate(HubProxy proxy); // return false: 登録を解除する
}
