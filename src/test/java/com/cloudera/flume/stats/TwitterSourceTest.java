package com.cloudera.flume.stats;

import static org.junit.Assert.*;

import org.apache.flume.Context;
import org.apache.log4j.Logger;
import org.junit.Before;
import org.junit.Test;

public class TwitterSourceTest {
	private static final Logger LOG = Logger.getLogger(TwitterSourceTest.class);

	@Before
	public void setUp() throws Exception {
		Context ctx = new Context();
	    ctx.put(TwitterSourceConstants.ACCESS_TOKEN_KEY, "5");
	    ctx.put(TwitterSourceConstants.ACCESS_TOKEN_SECRET_KEY, "10");
	    ctx.put(TwitterSourceConstants.CONSUMER_KEY_KEY, "5");
	    ctx.put(TwitterSourceConstants.CONSUMER_SECRET_KEY, "10");
	}

	@Test
	public void testTwitterSourceTest() {

	}

}
