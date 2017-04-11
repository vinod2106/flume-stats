package com.shavinod.flume.source;

import java.io.BufferedReader;
import java.nio.charset.UnsupportedCharsetException;
import java.util.ArrayList;
import java.util.List;
import javax.servlet.http.HttpServletRequest;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class BDHandler implements HTTPSourceHandler {

	private static final Logger LOG = LoggerFactory.getLogger(BDHandler.class);
	private String sCurrentLine;

	public List<Event> getEvents(HttpServletRequest request) throws Exception {
		BufferedReader reader = request.getReader();
		String charset = request.getCharacterEncoding();

		if (charset == null) {
			LOG.debug("Charset is null, default charset of UTF-8 will be used.");
			charset = "UTF-8";
		} else if (!(charset.equalsIgnoreCase("utf-8") || charset.equalsIgnoreCase("utf-16")
				|| charset.equalsIgnoreCase("utf-32"))) {
			LOG.error("Unsupported character set in request {}. " + "BDhandler supports UTF-8, "
					+ "UTF-16 and UTF-32 only.", charset);
			throw new UnsupportedCharsetException("BDhandler supports UTF-8, " + "UTF-16 and UTF-32 only.");
		}

		List<Event> eventList = new ArrayList<Event>(0);

		try {
			while ((sCurrentLine = reader.readLine()) != null) {
				System.out.println(sCurrentLine);
				Event event = EventBuilder.withBody(sCurrentLine.getBytes());
				eventList.add(event);
			}
		} catch (Exception ex) {
			System.out.println("Exception in Reading File" + ex.getMessage());
		}

		LOG.debug("No of events in the request : " + eventList.size());
		System.out.println("No of events in the request : " + eventList.size());
		return getSimpleEvents(eventList);
	}

	public void configure(Context context) {
	}

	private List<Event> getSimpleEvents(List<Event> events) {
		List<Event> newEvents = new ArrayList<Event>(events.size());
		for (Event e : events) {
			newEvents.add(EventBuilder.withBody(e.getBody(), e.getHeaders()));
		}
		return newEvents;
	}
}
