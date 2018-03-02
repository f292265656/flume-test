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
package com.test.flume.jsonsource;

import java.io.BufferedReader;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.servlet.http.HttpServletRequest;

import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.JSONEvent;
import org.apache.flume.source.http.HTTPBadRequestException;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * PlainJSONHandler for HTTPSource that accepts json-based http body.
 *
 * This handler throws exception if the deserialization fails because of bad
 * format or any other reason.
 */

public class PlainJSONHandler implements HTTPSourceHandler {

	private static final String FORWARD_HEADERS = "forwardHeaders";
	private static final Logger LOG = LoggerFactory.getLogger(PlainJSONHandler.class);
//	private static JsonParser parser = new JsonParser();
	private static Set<String> forwardHeaders = new HashSet<String>();

	@Override
	public void configure(Context context) {
		String confForwardHeaders = context.getString(FORWARD_HEADERS);
		if (confForwardHeaders != null) {
			if (forwardHeaders.addAll(Arrays.asList(confForwardHeaders.split(",")))) {
				LOG.debug("forwardHeaders=" + forwardHeaders);
			} else {
				LOG.error("error to get forward headers from " + confForwardHeaders);
			}
		} else {
			LOG.debug("no forwardHeaders");
		}

	}

	@Override
	public List<Event> getEvents(HttpServletRequest request) throws HTTPBadRequestException, Exception {
		Map<String, String> eventHeaders = new HashMap<String, String>();
		Enumeration requestHeaders = request.getHeaderNames();
		while (requestHeaders.hasMoreElements()) {
			String header = (String) requestHeaders.nextElement();
			if (forwardHeaders.contains(header)) {
				eventHeaders.put(header, request.getHeader(header));
			}
		}

		BufferedReader reader = request.getReader();
		List<Event> eventList = new ArrayList<Event>(1);
		// String line = reader.readLine();
		StringBuffer lineBuffer = new StringBuffer();
		boolean tag;
		do {
			lineBuffer.append(reader.readLine());
		} while (tag = reader.read() != -1);
		if (lineBuffer != null) {
			/*
			 * try { parser.parse(line); } catch (JsonParseException ex) { throw
			 * new HTTPBadRequestException(
			 * "HTTP body is not a valid JSON object.", ex); }
			 */
			Event event = new JSONEvent();
			event.setBody(lineBuffer.toString().getBytes());
			event.setHeaders(eventHeaders);
			eventList.add(event);

			LOG.debug("========= Event body:" + new String(event.getBody()) + "==============");
		}
		return eventList;
	}

}