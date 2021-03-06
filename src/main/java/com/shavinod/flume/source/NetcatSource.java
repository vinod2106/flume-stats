package com.shavinod.flume.source;
import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.net.InetSocketAddress;
import java.net.SocketAddress;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.channels.Channels;
import java.nio.channels.ClosedByInterruptException;
import java.nio.channels.ServerSocketChannel;
import java.nio.channels.SocketChannel;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.flume.ChannelException;
import org.apache.flume.Context;
import org.apache.flume.CounterGroup;
import org.apache.flume.Event;
import org.apache.flume.EventDrivenSource;
import org.apache.flume.FlumeException;
import org.apache.flume.Source;
import org.apache.flume.channel.ChannelProcessor;
import org.apache.flume.conf.Configurable;
import org.apache.flume.conf.Configurables;
import org.apache.flume.event.EventBuilder;
import org.apache.flume.lifecycle.LifecycleState;
import org.apache.flume.source.AbstractSource;
import org.apache.flume.source.NetcatSourceConfigurationConstants;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

/**
 * 
 * A netcat-like source that listens on a given port and turns each line of text
 * into an event.
 * 
 * 
 * This source, primarily built for testing and exceedingly simple systems, acts
 * like nc -k -l [host] [port]. In other words, it opens a specified
 * port and listens for data. The expectation is that the supplied data is
 * newline separated text. Each line of text is turned into a Flume event and
 * sent via the connected channel.
 * 
 *  Most testing has been done by using the nc client but other,
 * similarly implemented, clients should work just fine.
 * 
 * 
 * Configuration options
 * 
 * 
 * Parameter
 * Description
 * Unit / Type
 * Default
 * 
 * 
 * bind
 * The hostname or IP to which the source will bind.
 * Hostname or IP / String
 * none (required)
 * 
 * 
 * port
 * The port to which the source will bind and listen for events.
 * TCP port / int
 * none (required)
 * 
 * 
 * max-line-length
 * The maximum # of chars a line can be per event (including newline).
 * Number of UTF-8 characters / int
 * 512
 * 
 * 
 * 
 * Metrics
 * 
 * 
 * TODO
 * 
 */
public class NetcatSource extends AbstractSource implements Configurable,
    EventDrivenSource {

  private static final Logger logger = LoggerFactory
      .getLogger(NetcatSource.class);

  private String hostName;
  private int port;
  private int maxLineLength;
  private boolean ackEveryEvent;
  private String sourceEncoding;

  private CounterGroup counterGroup;
  private ServerSocketChannel serverSocket;
  private AtomicBoolean acceptThreadShouldStop;
  private Thread acceptThread;
  private ExecutorService handlerService;

  public NetcatSource() {
    super();

    port = 0;
    counterGroup = new CounterGroup();
    acceptThreadShouldStop = new AtomicBoolean(false);
  }

  @Override
  public void configure(Context context) {
    String hostKey = NetcatSourceConfigurationConstants.CONFIG_HOSTNAME;
    String portKey = NetcatSourceConfigurationConstants.CONFIG_PORT;
    String ackEventKey = NetcatSourceConfigurationConstants.CONFIG_ACKEVENT;

    Configurables.ensureRequiredNonNull(context, hostKey, portKey);

    hostName = context.getString(hostKey);
    port = context.getInteger(portKey);
    ackEveryEvent = context.getBoolean(ackEventKey, true);
    maxLineLength = context.getInteger(
        NetcatSourceConfigurationConstants.CONFIG_MAX_LINE_LENGTH,
        NetcatSourceConfigurationConstants.DEFAULT_MAX_LINE_LENGTH);
    sourceEncoding = context.getString(
        NetcatSourceConfigurationConstants.CONFIG_SOURCE_ENCODING,
        NetcatSourceConfigurationConstants.DEFAULT_ENCODING
    );
  }

  @Override
  public void start() {

    logger.info("Source starting");

    counterGroup.incrementAndGet("open.attempts");

    handlerService = Executors.newCachedThreadPool(new ThreadFactoryBuilder()
        .setNameFormat("netcat-handler-%d").build());

    try {
      SocketAddress bindPoint = new InetSocketAddress(hostName, port);

      serverSocket = ServerSocketChannel.open();
      serverSocket.socket().setReuseAddress(true);
      serverSocket.socket().bind(bindPoint);

      logger.info("Created serverSocket:{}", serverSocket);
    } catch (IOException e) {
      counterGroup.incrementAndGet("open.errors");
      logger.error("Unable to bind to socket. Exception follows.", e);
      throw new FlumeException(e);
    }

    AcceptHandler acceptRunnable = new AcceptHandler(maxLineLength);
    acceptThreadShouldStop.set(false);
    acceptRunnable.counterGroup = counterGroup;
    acceptRunnable.handlerService = handlerService;
    acceptRunnable.shouldStop = acceptThreadShouldStop;
    acceptRunnable.ackEveryEvent = ackEveryEvent;
    acceptRunnable.source = this;
    acceptRunnable.serverSocket = serverSocket;
    acceptRunnable.sourceEncoding = sourceEncoding;

    acceptThread = new Thread(acceptRunnable);

    acceptThread.start();

    logger.debug("Source started");
    super.start();
  }

  @Override
  public void stop() {
    logger.info("Source stopping");

    acceptThreadShouldStop.set(true);

    if (acceptThread != null) {
      logger.debug("Stopping accept handler thread");

      while (acceptThread.isAlive()) {
        try {
          logger.debug("Waiting for accept handler to finish");
          acceptThread.interrupt();
          acceptThread.join(500);
        } catch (InterruptedException e) {
          logger
              .debug("Interrupted while waiting for accept handler to finish");
          Thread.currentThread().interrupt();
        }
      }

      logger.debug("Stopped accept handler thread");
    }

    if (serverSocket != null) {
      try {
        serverSocket.close();
      } catch (IOException e) {
        logger.error("Unable to close socket. Exception follows.", e);
        return;
      }
    }

    if (handlerService != null) {
      handlerService.shutdown();

      logger.debug("Waiting for handler service to stop");

      // wait 500ms for threads to stop
      try {
        handlerService.awaitTermination(500, TimeUnit.MILLISECONDS);
      } catch (InterruptedException e) {
        logger
            .debug("Interrupted while waiting for netcat handler service to stop");
        Thread.currentThread().interrupt();
      }

      if (!handlerService.isShutdown()) {
        handlerService.shutdownNow();
      }

      logger.debug("Handler service stopped");
    }

    logger.debug("Source stopped. Event metrics:{}", counterGroup);
    super.stop();
  }

  private static class AcceptHandler implements Runnable {

    private ServerSocketChannel serverSocket;
    private CounterGroup counterGroup;
    private ExecutorService handlerService;
    private EventDrivenSource source;
    private AtomicBoolean shouldStop;
    private boolean ackEveryEvent;
    private String sourceEncoding;

    private final int maxLineLength;

    public AcceptHandler(int maxLineLength) {
      this.maxLineLength = maxLineLength;
    }

    @Override
    public void run() {
      logger.debug("Starting accept handler");

      while (!shouldStop.get()) {
        try {
          SocketChannel socketChannel = serverSocket.accept();

          NetcatSocketHandler request = new NetcatSocketHandler(maxLineLength);

          request.socketChannel = socketChannel;
          request.counterGroup = counterGroup;
          request.source = source;
          request.ackEveryEvent = ackEveryEvent;
          request.sourceEncoding = sourceEncoding;

          handlerService.submit(request);

          counterGroup.incrementAndGet("accept.succeeded");
        } catch (ClosedByInterruptException e) {
          // Parent is canceling us.
        } catch (IOException e) {
          logger.error("Unable to accept connection. Exception follows.", e);
          counterGroup.incrementAndGet("accept.failed");
        }
      }

      logger.debug("Accept handler exiting");
    }
  }

  private static class NetcatSocketHandler implements Runnable {

    private Source source;
    private CounterGroup counterGroup;
    private SocketChannel socketChannel;
    private boolean ackEveryEvent;
    private String sourceEncoding;

    private final int maxLineLength;

    public NetcatSocketHandler(int maxLineLength) {
      this.maxLineLength = maxLineLength;
    }

    @Override
    public void run() {
      logger.debug("Starting connection handler");
      Event event = null;

      try {
        Reader reader = Channels.newReader(socketChannel, sourceEncoding);
        Writer writer = Channels.newWriter(socketChannel, sourceEncoding);
        CharBuffer buffer = CharBuffer.allocate(maxLineLength);
        buffer.flip(); // flip() so fill() sees buffer as initially empty

        while (true) {
          // this method blocks until new data is available in the socket
          int charsRead = fill(buffer, reader);
          logger.debug("Chars read = {}", charsRead);

          // attempt to process all the events in the buffer
          int eventsProcessed = processEvents(buffer, writer);
          logger.debug("Events processed = {}", eventsProcessed);

          if (charsRead == -1) {
            // if we received EOF before last event processing attempt, then we
            // have done everything we can
            break;
          } else if (charsRead == 0 && eventsProcessed == 0) {
            if (buffer.remaining() == buffer.capacity()) {
              // If we get here it means:
              // 1. Last time we called fill(), no new chars were buffered
              // 2. After that, we failed to process any events => no newlines
              // 3. The unread data in the buffer == the size of the buffer
              // Therefore, we are stuck because the client sent a line longer
              // than the size of the buffer. Response: Drop the connection.
              logger.warn("Client sent event exceeding the maximum length");
              counterGroup.incrementAndGet("events.failed");
              writer.write("FAILED: Event exceeds the maximum length (" +
                  buffer.capacity() + " chars, including newline)\n");
              writer.flush();
              break;
            }
          }
        }

        socketChannel.close();

        counterGroup.incrementAndGet("sessions.completed");
      } catch (IOException e) {
        counterGroup.incrementAndGet("sessions.broken");
        try {
          socketChannel.close();
        } catch (IOException ex) {
          logger.error("Unable to close socket channel. Exception follows.", ex);
        }
      }

      logger.debug("Connection handler exiting");
    }

    /**
     * Consume some number of events from the buffer into the system.
     *
     * Invariants (pre- and post-conditions): 
     *   buffer should have position @ beginning of unprocessed data. 
     *   buffer should have limit @ end of unprocessed data. 
     *
     * @param buffer The buffer containing data to process
     * @param writer The channel back to the client
     * @return number of events successfully processed
     * @throws IOException
     */
    private int processEvents(CharBuffer buffer, Writer writer)
        throws IOException {

      int numProcessed = 0;

      boolean foundNewLine = true;
      while (foundNewLine) {
        foundNewLine = false;

        int limit = buffer.limit();
        for (int pos = buffer.position(); pos < limit; pos++) {
          if (buffer.get(pos) == '\n') {

            // parse event body bytes out of CharBuffer
            buffer.limit(pos); // temporary limit
            ByteBuffer bytes = Charsets.UTF_8.encode(buffer);
            buffer.limit(limit); // restore limit

            // build event object
            byte[] body = new byte[bytes.remaining()];
            bytes.get(body);
            Event event = EventBuilder.withBody(body);

            // process event
            ChannelException ex = null;
            try {
              source.getChannelProcessor().processEvent(event);
            } catch (ChannelException chEx) {
              ex = chEx;
            }

            if (ex == null) {
              counterGroup.incrementAndGet("events.processed");
              numProcessed++;
              if (true == ackEveryEvent) {
                writer.write("OK\n");
              }
            } else {
              counterGroup.incrementAndGet("events.failed");
              logger.warn("Error processing event. Exception follows.", ex);
              writer.write("FAILED: " + ex.getMessage() + "\n");
            }
            writer.flush();

            // advance position after data is consumed
            buffer.position(pos + 1); // skip newline
            foundNewLine = true;

            break;
          }
        }

      }

      return numProcessed;
    }

    /**
     * Refill the buffer read from the socket.
     *
     * Preconditions: 
     *   buffer should have position @ beginning of unprocessed data. 
     *   buffer should have limit @ end of unprocessed data. 
     *
     * Postconditions: 
     *   buffer should have position @ beginning of buffer (pos=0). 
     *   buffer should have limit @ end of unprocessed data. 
     *
     * Note: this method blocks on new data arriving.
     *
     * @param buffer The buffer to fill
     * @param reader The Reader to read the data from
     * @return number of characters read
     * @throws IOException
     */
    private int fill(CharBuffer buffer, Reader reader)
        throws IOException {

      // move existing data to the front of the buffer
      buffer.compact();

      // pull in as much data as we can from the socket
      int charsRead = reader.read(buffer);
      counterGroup.addAndGet("characters.received", Long.valueOf(charsRead));

      // flip so the data can be consumed
      buffer.flip();

      return charsRead;
    }

  }

@Override
public ChannelProcessor getChannelProcessor() {
	// TODO Auto-generated method stub
	return null;
}

@Override
public void setChannelProcessor(ChannelProcessor arg0) {
	// TODO Auto-generated method stub
	
}

@Override
public LifecycleState getLifecycleState() {
	// TODO Auto-generated method stub
	return null;
}

@Override
public String getName() {
	// TODO Auto-generated method stub
	return null;
}

@Override
public void setName(String arg0) {
	// TODO Auto-generated method stub
	
}
}