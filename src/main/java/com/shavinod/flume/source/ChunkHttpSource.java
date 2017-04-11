package com.shavinod.flume.source;

import org.apache.flume.instrumentation.SourceCounter;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;
import javax.servlet.http.HttpServletRequest;
import org.apache.flume.Context;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.ConfigurationException;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.http.HTTPSourceHandler;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ChunkHttpSource extends AbstractSource implements EventDrivenSource, Configurable {
	private static final Logger LOGGER = LoggerFactory.getLogger(ChunkHttpSource.class);
	private SourceCounter counter;
	private int blobLength = BLOB_LENGTH_DEFAULT;
	public static final String BLOB_LENGTH_KEY = "blobLength";
	public static final int BLOB_LENGTH_DEFAULT = 1 * 1000 * 1000; // 100 mb
	public TimeZone timeZone;

	public void configure(Context context) {
		// intialize the counter
		this.counter = new SourceCounter(this.getName());
		this.blobLength = context.getInteger(BLOB_LENGTH_KEY, BLOB_LENGTH_DEFAULT);
		LOGGER.debug(" ############ TimeZone actual: " + new Date());
		String tzName = "UTC";
		this.timeZone = (tzName == null) ? null : TimeZone.getTimeZone(tzName);
		LOGGER.debug(" ############ TimeZone new : " + new Date());

		if (this.blobLength <= 0) {
			try {
				throw new ConfigurationException(
						"Configuration parameter " + BLOB_LENGTH_KEY + " must be greater than 0 : " + blobLength);
			} catch (ConfigurationException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		LOGGER.info(" ########### Current blobLength : " + blobLength);

	}

	@Override
	public synchronized void start() {
		// TODO Auto-generated method stub
		this.counter.start();
		super.start();
		this.counter.setOpenConnectionCount(1);

	}

	@Override
	public synchronized void stop() {
		// TODO Auto-generated method stub
		this.counter.setOpenConnectionCount(0);
		super.stop();
		this.counter.stop();
	}

	public class BDMPHandler implements HTTPSourceHandler {

		@Override
		public void configure(Context arg0) {
			// TODO Auto-generated method stub

		}

		@Override
		// using chunks
		public List<Event> getEvents(HttpServletRequest request) throws IOException {
			InputStream in = null;
			List<Event> eventList = new ArrayList<Event>();

			LOGGER.info("################# Intialize byte array buf of size (bytes) : " + blobLength);

			try {

				in = request.getInputStream();

				Map<String, String> headers = extractHTTPHeaders(request);

				String charset = request.getCharacterEncoding();

				charset = validateCharset(charset);

				byte[] buf = new byte[blobLength];

				int bytesRead = -1;
				do {
					int index = 0;

					for (; (bytesRead = in.read(buf, index, blobLength - index)) > 0; index += bytesRead)
						;

					Event event = EventBuilder.withBody(buf, headers);

					LOGGER.info("############# index: " + index);

					eventList.add(event);

					LOGGER.info("############# eventList tmp size : " + eventList.size());

				} while (bytesRead >= 0);

				LOGGER.info("############# eventList final size : " + eventList.size());

			} catch (Exception e) {
				e.printStackTrace();
			} finally {

				in.close();
			}

			return eventList;
		}

		private Map<String, String> extractHTTPHeaders(HttpServletRequest request) {
			Map<String, String> httpHeaders = new HashMap<String, String>();

			Enumeration<String> headerNames = request.getHeaderNames();

			while (headerNames.hasMoreElements()) {
				String hName = headerNames.nextElement();
				httpHeaders.put(hName, request.getHeader(hName));
			}
			return httpHeaders;
		}

		private String validateCharset(String charset) {
			if (charset == null) {
				LOGGER.debug("Charset is null, default charset of UTF-8 will be used.");

				charset = "UTF-8";
			} else if (!("utf-8".equalsIgnoreCase(charset) || "utf-16".equalsIgnoreCase(charset)
					|| "utf-32".equalsIgnoreCase(charset))) {

				LOGGER.error(
						"Unsupported character set in request. BDMP handler supports UTF-8, UTF-16 and UTF-32 only.",
						charset);

			}
			return charset;
		}

	}

}
