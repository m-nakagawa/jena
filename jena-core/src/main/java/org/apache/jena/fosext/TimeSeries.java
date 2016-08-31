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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.PrintWriter;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.Iterator;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Stream;
import java.util.zip.DataFormatException;

import org.slf4j.Logger;

/**
 * @author m-nakagawa
 *
 */
public class TimeSeries {
    private final static Logger log = getLogger(RealtimeValueBroker.class);

	private static final Pattern fosProxyLeafOrArray = Pattern.compile(FosNames.FOS_PROXY_HUB+"(.+)$");
	private static final String STORE_ROOT = "./run/timeseries/";
	{
		Path path = Paths.get(STORE_ROOT);
		path.toFile().mkdir();
	}

	private final String iri;
	private final String id;
	private Path filePath;
	private PrintWriter writer = null;
	
	public TimeSeries(String iri) throws DataFormatException {
		this.iri = iri;
		Matcher m = fosProxyLeafOrArray.matcher(iri);
		if(m.matches()){
			this.id = m.group(1);
			this.filePath = Paths.get(STORE_ROOT+this.id);
			try {
				this.writer = new PrintWriter(new BufferedWriter(new OutputStreamWriter(new FileOutputStream(this.filePath.toFile(), true), "UTF-8")));
			} catch(Exception e){
				log.error(e.toString()+":"+this.filePath.toAbsolutePath().toString());
			}
		}
		else {
			throw new DataFormatException("Illegal proxy name:"+this.iri);
		}
	}
	
	public void write(String data){
		if(this.writer != null){
			this.writer.println(data);
			this.writer.flush();
		}
	}
	
	private BufferedReader getReader(){
		BufferedReader reader = null;
		try {
			reader = new BufferedReader(new InputStreamReader(new FileInputStream(this.filePath.toFile()),"UTF-8"));
		}catch(Exception e){
			log.error(e.toString()+":"+this.filePath.toString());
		}
		return reader;
	}
	
	public Stream<String> getHistory(){
		this.writer.flush();
		BufferedReader reader = this.getReader();
		if(reader != null){
			return reader.lines();
		}
		else {
			return Stream.of();
		}
	}
	
	
	/**
	 * 最後のlimit件をとりだす
	 * @param limit
	 * @return
	 */
	public Stream<String> getHistory(int limit){
		if(limit < 0){
			throw new IllegalArgumentException();
		}
		if(limit == 0){
			return getHistory();
		}
		String[] buffer = new String[limit];
		boolean full = false;
		int tail = 0;
		try(Stream<String> source = this.getHistory();){
			for(Iterator<String> i=source.iterator(); i.hasNext(); ){
				buffer[tail++] = i.next();
				if(tail == limit){
					full = true;
					tail = 0;
				}
			}
		}
		Stream<String> ret;
		if(full){
			ret = Arrays.stream(buffer, tail, limit);
			if(tail != 0){
				ret = Stream.concat(ret, Arrays.stream(buffer, 0, tail));
			}
		}
		else {
			ret = Arrays.stream(buffer, 0, tail);
		}
		return ret;
	}
}
