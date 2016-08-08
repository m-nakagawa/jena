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

package org.apache.jena.fuseki.sosext;

import java.io.IOException;

import org.eclipse.jetty.websocket.api.Session;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketConnect;
import org.eclipse.jetty.websocket.api.annotations.OnWebSocketMessage;
import org.eclipse.jetty.websocket.api.annotations.WebSocket;

// MNakagawa
@WebSocket
public class MyWebSocket {
	private Session session;

	@OnWebSocketConnect
	public void onConnect(Session session) {
		this.session = session;
		System.out.println("Connect");
		try {
			session.getRemote().sendString("Hello.");
		}
		catch(IOException ex){
			System.out.println("Connect: IOException"+ex.toString());
		}
	}

	@OnWebSocketMessage
	public void onText(String message) {
		System.out.println("onMessage: " + message);
		// エコーする
		try {
			this. session.getRemote().sendString("Ans: "+message);
		}
		catch(IOException ex){
			System.out.println("Connect: IOException"+ex.toString());
		}
	}

	/*
	private Session session;
	
	@OnWebSocketConnect
	public void onConnect(Session session) {
		this.session = session;
		System.out.println("Connect");
		try {
			session.getBasicRemote().sendText("Hello.");
		}
		catch(IOException ex){
			System.out.println("Connect: IOException"+ex.toString());
		}
	}

	@OnWebSocketMessage
	public void onText(String message) {
		System.out.println("onMessage: " + message);
		// エコーする
		try {
			this.session.getBasicRemote().sendText("Ans: "+message);
		}
		catch(IOException ex){
			System.out.println("Connect: IOException"+ex.toString());
		}
	}

	@OnWebSocketClose
	public void onClose(int statusCode, String reason) {
		System.out.println("Close:"+reason);
	}
	*/
}

