package com.gboxsw.acpmod.gep;

import java.io.*;
import java.util.logging.*;

/**
 * Thread-safe implementation of messenger implementing the GEP.
 */
public class GEPMessenger {

	/**
	 * Interface representing a full-duplex stream.
	 */
	public interface FullDuplexStream extends Closeable {
		/**
		 * Returns the input stream of the full-duplex stream.
		 * 
		 * @return the input stream.
		 */
		public InputStream getInputStream();

		/**
		 * Returns the output stream of the full-duplex stream.
		 * 
		 * @return the output stream.
		 */
		public OutputStream getOutputStream();

		/**
		 * Closes the full-duplex stream.
		 */
		public void close();
	}

	/**
	 * Interface to access network or remote side over a thread-safe stream
	 * session.
	 */
	public interface FullDuplexStreamSocket {
		/**
		 * Creates a new full duplex stream.
		 * 
		 * @return the open full-duplex stream.
		 * 
		 * @throws IOException
		 *             when construction and opening of full-duplex stream
		 *             failed.
		 */
		public FullDuplexStream createStream() throws IOException;
	}

	/**
	 * Listener for receiving messages sent over the channel.
	 */
	public interface MessageListener {
		/**
		 * Invoked when a message is received.
		 * 
		 * @param tag
		 *            the tag of message or a negative number, if the received
		 *            message does not contain a tag
		 * @param message
		 *            the message
		 */
		void onMessageReceived(int tag, byte[] message);
	}

	/**
	 * Name of the communication thread.
	 */
	private static final String THREAD_NAME = "GEPMessenger - Communication thread";

	/**
	 * Logger.
	 */
	private static final Logger logger = Logger.getLogger(GEPMessenger.class.getName());

	/**
	 * Delay (in milliseconds) between two attempts to connect.
	 */
	private final long RECONNECT_DELAY = 5000;

	/**
	 * Byte indicating start of a new message
	 */
	private final int MESSAGE_START_BYTE = 0x0C;

	/**
	 * Byte indicating end of the message without tag
	 */
	private final int MESSAGE_END_BYTE = 0x03;

	/**
	 * Byte indicating end of the message with tag
	 */
	private final int MESSAGE_END_WITH_TAG_BYTE = 0x06;

	/**
	 * States of implementation during receiving bytes according to the
	 * protocol.
	 */
	private enum ProtocolState {
		/**
		 * Waits for the beginning of a new message
		 */
		WAIT_START,
		/**
		 * Waits for a byte encoding the destination id of the message
		 */
		WAIT_DESTINATION_ID,
		/**
		 * Waits for a high nibble of the next message byte
		 */
		WAIT_HIGH_NIBBLE,
		/**
		 * Waits for a low nibble of the next message byte
		 */
		WAIT_LOW_NIBBLE,
		/**
		 * Waits for a CRC byte after receiving marker indicating end of a
		 * message without a tag
		 */
		WAIT_CRC,
		/**
		 * Waits for a CRC byte after receiving marker indicating end of a
		 * message with a tag
		 */
		WAIT_CRC_WITH_TAG
	}

	/**
	 * Listener for receiving messages.
	 */
	private final MessageListener messageListener;

	/**
	 * The socket for accessing network or remote side.
	 */
	private final FullDuplexStreamSocket socket;

	/**
	 * Maximal length of a message.
	 */
	private final int maxMessageLength;

	/**
	 * Identifier of the messenger applied to filter incoming messages. If id is
	 * set to 0, all messages are received.
	 */
	private final byte messengerId;

	/**
	 * Precomputed table for CRC checksums.
	 */
	private final short[] crcTable = new short[256];

	/**
	 * Thread for receiving and sending messages. Null, if the messenger is not
	 * running.
	 */
	private Thread communicationThread;

	/**
	 * Sets whether connection should be reconnected if failed.
	 */
	private boolean automaticReconnect = true;

	/**
	 * Indicates that the communication thread is a daemon thread.
	 */
	private boolean daemon;

	/**
	 * Indicates that the messenger is in connected state, i.e., it is ready to
	 * send and receive messages.
	 */
	private boolean connected;

	/**
	 * Full-duplex stream for sending and receiving bytes from remote side or a
	 * network.
	 */
	private FullDuplexStream ioStream;

	/**
	 * Indicates that messenger thread should be terminated as soon as possible.
	 */
	private volatile boolean stopFlag = false;

	/**
	 * Indicates that the messenger is connecting (for the first time, i.e., not
	 * after connection has been broken).
	 */
	private boolean firstConnecting = false;

	/**
	 * Internal synchronization lock.
	 */
	private final Object lock = new Object();

	/**
	 * Lock ensuring serialization of messages transmissions.
	 */
	private final Object sendSerializationLock = new Object();

	/**
	 * Constructs a messenger.
	 * 
	 * @param socket
	 *            the stream socket for accessing network or remote side.
	 * @param messengerId
	 *            the identifier of the messenger applied to filter incoming
	 *            messages. Allowed values are between 0 and 15, if the value is
	 *            0, all messages are received.
	 * @param maxMessageLength
	 *            the maximal accepted length of a message.
	 * @param messageListener
	 *            the message listener.
	 */
	public GEPMessenger(FullDuplexStreamSocket socket, int messengerId, int maxMessageLength,
			MessageListener messageListener) {
		if ((messengerId < 0) || (messengerId > 15)) {
			throw new RuntimeException("Invalid identifier of messenger (allowed: 0-15).");
		}

		this.socket = socket;
		this.maxMessageLength = Math.abs(maxMessageLength);
		this.messengerId = (byte) messengerId;
		this.messageListener = messageListener;
		this.daemon = true;

		initializeCRCTable();
	}

	/**
	 * Returns whether communication thread of the messenger is a daemon thread.
	 * 
	 * @return true, if the communication thread is marked as a daemon thread.
	 */
	public boolean isDaemon() {
		synchronized (lock) {
			return daemon;
		}
	}

	/**
	 * Marks the communication thread of the messenger as either a daemon thread
	 * or a user thread.
	 * 
	 * @param daemon
	 *            if true, marks the communication thread as a daemon thread.
	 */
	public void setDaemon(boolean daemon) {
		synchronized (lock) {
			if (isRunning()) {
				throw new IllegalStateException("The messenger is running. The method cannot be invoked.");
			}
			this.daemon = daemon;
		}
	}

	/**
	 * Returns whether the client will automatically attempt to reconnect, if
	 * the connection is lost.
	 * 
	 * @return true, if automatic reconnect is enabled, false otherwise.
	 */
	public boolean isAutomaticReconnect() {
		synchronized (lock) {
			return automaticReconnect;
		}
	}

	/**
	 * Sets whether the client will automatically attempt to reconnect, if the
	 * connection is lost.
	 * 
	 * @param automaticReconnect
	 *            if set to true, automatic reconnect will be enabled.
	 */
	public void setAutomaticReconnect(boolean automaticReconnect) {
		synchronized (lock) {
			if (isRunning()) {
				throw new IllegalStateException("The messenger is running. The method cannot be invoked.");
			}
			this.automaticReconnect = automaticReconnect;
		}
	}

	/**
	 * Returns whether the messenger is running.
	 * 
	 * @return true, if the messenger is running, false otherwise.
	 */
	public boolean isRunning() {
		synchronized (lock) {
			return (communicationThread != null);
		}
	}

	/**
	 * Returns whether the messenger is connected, i.e., it is ready to send and
	 * receive messages.
	 * 
	 * @return true, if the messenger is connected, false otherwise.
	 */
	public boolean isConnected() {
		synchronized (lock) {
			return connected;
		}
	}

	/**
	 * Starts the messenger, if the messenger is not running.
	 * 
	 * @param waitForConnection
	 *            true, if the method is blocked until connection is
	 *            established, false otherwise.
	 */
	public void start(boolean waitForConnection) {
		synchronized (lock) {
			if (isRunning()) {
				throw new RuntimeException("Messenger is already running.");
			}

			// create communication thread
			communicationThread = new Thread(new Runnable() {
				@Override
				public void run() {
					communicate();
				}
			}, "GEPMessenger");

			// start the communication thread
			stopFlag = false;
			firstConnecting = true;
			communicationThread.setDaemon(daemon);
			communicationThread.setName(THREAD_NAME);
			communicationThread.start();

			// if required, wait for establishing a connection
			if (waitForConnection) {
				while (firstConnecting) {
					try {
						lock.wait();
					} catch (InterruptedException ignore) {

					}
				}
			}
		}
	}

	/**
	 * Executes code of the communication thread.
	 */
	private void communicate() {
		try {
			while (!stopFlag) {
				connectAndReadMessages();
				if (automaticReconnect) {
					try {
						Thread.sleep(RECONNECT_DELAY);
					} catch (InterruptedException ignore) {
						// interrupted exceptions are ignored
					}
				} else {
					break;
				}
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Unexpected state or error on GEP connection.", e);
		} finally {
			synchronized (lock) {
				// store that there is no running communication
				// thread
				communicationThread = null;
			}
		}
	}

	/**
	 * Stops the messenger.
	 * 
	 * @param blocked
	 *            true, if the current thread should be blocked until the
	 *            communication terminates, false otherwise.
	 * @throws InterruptedException
	 *             if any thread has interrupted the current thread during
	 *             waiting for stopping of the messenger.
	 */
	public void stop(boolean blocked) throws InterruptedException {
		Thread waitThread = null;

		synchronized (lock) {
			if (communicationThread == null) {
				return;
			}

			waitThread = communicationThread;
			stopFlag = true;
			if (ioStream != null) {
				ioStream.close();
			}
			communicationThread.interrupt();
			lock.notifyAll();
		}

		if (waitThread != null) {
			if ((blocked) && (waitThread != Thread.currentThread())) {
				waitThread.join();
			}
		}
	}

	/**
	 * Sends a message and returns a send request objects that provides status
	 * information. The method is blocked until the message is sent.
	 * 
	 * @param destinationId
	 *            the identifier of the receiver. Allowed values are between 0
	 *            and 15, 0 stands for broadcasted message.
	 * @param message
	 *            the binary message.
	 * @param tag
	 *            the tag to be associated with the message.
	 * @return true, if the message has been sent, false otherwise.
	 * @throws RuntimeException
	 *             when message cannot be sent due to reasons different from the
	 *             state of communication channel.
	 */
	public boolean sendMessage(int destinationId, byte[] message, int tag) {
		// check tag
		if (tag >= 256 * 256) {
			throw new IllegalArgumentException("Message tag cannot be greater than 65535.");
		}

		// check destination id.
		if ((destinationId < 0) || (destinationId >= 16)) {
			throw new IllegalArgumentException("Invalid destination id. Only values between 0 and 15 are allowed.");
		}

		// get output stream
		final OutputStream outputStream;
		synchronized (lock) {
			if (communicationThread == null) {
				throw new RuntimeException("Messenger is not running.");
			}

			if (ioStream != null) {
				outputStream = ioStream.getOutputStream();
			} else {
				outputStream = null;
			}
		}

		// message cannot be sent, if connection is not open.
		if (ioStream == null) {
			return false;
		}

		// ensure that only one message is sending
		synchronized (sendSerializationLock) {
			return encodeAndSendMessage(outputStream, (short) destinationId, message, tag);
		}
	}

	/**
	 * Sends a message and returns a send request objects that provides status
	 * information. The method is blocked until the message is sent.
	 *
	 * @param destinationId
	 *            the identifier of the receiver. Allowed values are between 0
	 *            and 15, 0 stands for broadcasted message.
	 * @param message
	 *            the binary message.
	 * @return true, if the message has been sent, false otherwise.
	 * @throws RuntimeException
	 *             when message cannot be sent due to reasons different from the
	 *             state of communication channel.
	 */
	public boolean sendMessage(int destinationId, byte[] message) {
		return sendMessage(destinationId, message, -1);
	}

	/**
	 * Encodes and sends a message.
	 * 
	 * @param outputStream
	 *            the output stream where the message content (request) will be
	 *            written.
	 * @param destinationId
	 *            the identifier of the receiver.
	 * @param message
	 *            the binary message.
	 * @param tag
	 *            the tag to be associated with the message.
	 * @return true, if the message has been sent, false otherwise.
	 */
	private boolean encodeAndSendMessage(OutputStream outputStream, short destinationId, byte[] message, int tag) {
		try {
			short crc = 0;
			// write byte starting a message
			outputStream.write(MESSAGE_START_BYTE);

			// write destination id
			int encodedDestId = destinationId * 16 + ((destinationId ^ 0x0F) & 0x0F);
			outputStream.write(encodedDestId);
			crc = updateCRC(destinationId, crc);

			// compute length of data to be sent (including encoded tag)
			int rawMessageLength = (message == null) ? 0 : message.length;
			if (tag >= 0) {
				rawMessageLength += 2;
			}

			// create raw message content
			final int[] rawMessage = new int[rawMessageLength];
			int writeIdx = 0;
			if (message != null) {
				for (final byte b : message) {
					rawMessage[writeIdx] = b & 0xff;
					writeIdx++;
				}
			}

			if (tag >= 0) {
				rawMessage[writeIdx] = tag / 256;
				writeIdx++;
				rawMessage[writeIdx] = tag % 256;
			}

			// write message content with each byte encoded as two nibbles
			final int[] twoNibbles = new int[2];
			for (final int b : rawMessage) {
				crc = updateCRC((short) b, crc);
				twoNibbles[0] = b / 16;
				twoNibbles[1] = b % 16;
				twoNibbles[0] = twoNibbles[0] * 16 + ((twoNibbles[0] ^ 0x0F) & 0x0F);
				twoNibbles[1] = twoNibbles[1] * 16 + ((twoNibbles[1] ^ 0x0F) & 0x0F);

				outputStream.write(twoNibbles[0]);
				outputStream.write(twoNibbles[1]);
			}

			if (tag >= 0) {
				outputStream.write(MESSAGE_END_WITH_TAG_BYTE);
			} else {
				outputStream.write(MESSAGE_END_BYTE);
			}

			outputStream.write(crc);
			outputStream.flush();
			return true;
		} catch (Exception e) {
			return false;
		}
	}

	/**
	 * Core method implementing communication using the protocol. The method
	 * opens the session and reads incoming data (messages).
	 */
	private void connectAndReadMessages() {
		// specify maximal buffer size (message length + 2 bytes for tag)
		final int maxBufferSize = maxMessageLength + 2;
		// full duplex stream used in this session.
		FullDuplexStream sessionStream = null;
		try {
			// open session as a full-duplex stream
			try {
				sessionStream = socket.createStream();
			} catch (Exception e) {
				logger.log(Level.SEVERE, "Unable to open connection.", e);
				return;
			}
			final InputStream inputStream = sessionStream.getInputStream();

			// save that messenger is connected (has a stream).
			synchronized (lock) {
				firstConnecting = false;
				connected = true;
				this.ioStream = sessionStream;
				lock.notifyAll();
			}

			// start waiting for a message start
			ProtocolState state = ProtocolState.WAIT_START;

			byte[] inputBuffer = new byte[512];
			short[] messageBuffer = new short[maxBufferSize];
			int receivedMessageBytes = 0;
			short crc = 0;

			while (!stopFlag) {
				// read received data
				int receivedBytes;
				try {
					receivedBytes = inputStream.read(inputBuffer);
				} catch (Exception e) {
					// handle exception as end of data
					receivedBytes = -1;
				}

				if (receivedBytes < 0) {
					break;
				}

				// process received bytes
				dataloop: for (int i = 0; i < receivedBytes; i++) {
					int receivedByte = inputBuffer[i] & 0xFF;
					switch (state) {
					case WAIT_START: {
						if (receivedByte == MESSAGE_START_BYTE) {
							state = ProtocolState.WAIT_DESTINATION_ID;
						}
						continue dataloop;
					}

					case WAIT_DESTINATION_ID: {
						final int nibble = receivedByte / 16;
						if (nibble == ((receivedByte ^ 0x0F) & 0x0F)) {
							// skip message, if this messenger is not
							// destination of the message
							if ((messengerId > 0) && (nibble > 0) && (nibble != messengerId)) {
								state = ProtocolState.WAIT_START;
								continue dataloop;
							}

							// initialize receive of message content
							crc = updateCRC((short) nibble, (short) 0);
							receivedMessageBytes = 0;
							state = ProtocolState.WAIT_HIGH_NIBBLE;
						} else {
							state = ProtocolState.WAIT_START;
						}
						break;
					}

					case WAIT_HIGH_NIBBLE: {
						if (receivedByte == MESSAGE_END_BYTE) {
							state = ProtocolState.WAIT_CRC;
						} else if (receivedByte == MESSAGE_END_WITH_TAG_BYTE) {
							state = ProtocolState.WAIT_CRC_WITH_TAG;
						} else if (receivedMessageBytes >= maxBufferSize) {
							state = ProtocolState.WAIT_START;
						} else {
							final int nibble = receivedByte / 16;
							if (nibble == ((receivedByte ^ 0x0F) & 0x0F)) {
								messageBuffer[receivedMessageBytes] = (short) (nibble * 16);
								receivedMessageBytes++;
								state = ProtocolState.WAIT_LOW_NIBBLE;
							} else {
								state = ProtocolState.WAIT_START;
							}
						}
						break;
					}

					case WAIT_LOW_NIBBLE: {
						final int nibble = receivedByte / 16;
						if (nibble == ((receivedByte ^ 0x0F) & 0x0F)) {
							messageBuffer[receivedMessageBytes - 1] += nibble;
							crc = updateCRC(messageBuffer[receivedMessageBytes - 1], crc);
							state = ProtocolState.WAIT_HIGH_NIBBLE;
						} else {
							state = ProtocolState.WAIT_START;
						}
						break;
					}

					case WAIT_CRC: {
						if (receivedByte == crc) {
							handleReceivedMessage(messageBuffer, receivedMessageBytes, -1);
							state = ProtocolState.WAIT_START;
							continue dataloop;
						}
						state = ProtocolState.WAIT_START;
						break;
					}

					case WAIT_CRC_WITH_TAG: {
						if ((receivedByte == crc) && (receivedMessageBytes >= 2)) {
							final int messageTag = messageBuffer[receivedMessageBytes - 2] * 256
									+ messageBuffer[receivedMessageBytes - 1];
							receivedMessageBytes -= 2;
							handleReceivedMessage(messageBuffer, receivedMessageBytes, messageTag);
							state = ProtocolState.WAIT_START;
							continue dataloop;
						}
						state = ProtocolState.WAIT_START;
						break;
					}

					} // end of switch

					// make progress from WAIT_START, if the received by is
					// MESSAGE_START_BYTE
					if ((state == ProtocolState.WAIT_START) && (receivedByte == MESSAGE_START_BYTE)) {
						state = ProtocolState.WAIT_DESTINATION_ID;
					}
				}
			}
		} catch (Exception e) {
			logger.log(Level.SEVERE, "Communication failed or disconnected.", e);
		} finally {
			if (sessionStream != null) {
				try {
					sessionStream.close();
				} catch (Exception e) {
					logger.log(Level.SEVERE, "Closing of session stream failed.", e);
				}
			}

			// set disconnected state
			synchronized (lock) {
				firstConnecting = false;
				connected = false;
				this.ioStream = null;
				lock.notifyAll();
			}
		}
	}

	/**
	 * Handles a received message.
	 * 
	 * @param messageBuffer
	 *            the buffer that stores the received message.
	 * @param messageLength
	 *            the length of the received message.
	 * @param tag
	 *            the tag associated with the message.
	 */
	private void handleReceivedMessage(short[] messageBuffer, int messageLength, int tag) {
		byte[] message = new byte[messageLength];
		for (int i = 0; i < messageLength; i++) {
			message[i] = (byte) messageBuffer[i];
		}

		if (messageListener != null) {
			messageListener.onMessageReceived(tag, message);
		}
	}

	/**
	 * Updates CRC8 checksum after adding a new byte of data.
	 * 
	 * @param data
	 *            the new byte of data
	 * @param crc
	 *            the current CRC8 checksum
	 * @return updates CRC8 checksum
	 */
	private short updateCRC(short aByte, short crc) {
		return (short) (crcTable[(aByte ^ crc) & 0xFF]);
	}

	/**
	 * Initializes table for fast computation of CRC8 checksums
	 */
	private void initializeCRCTable() {
		// based on:
		// http://stackoverflow.com/questions/25284556/translate-crc8-from-c-to-java
		final int polynomial = 0x8C;
		for (int dividend = 0; dividend < 256; dividend++) {
			int remainder = dividend;// << 8;
			for (int bit = 0; bit < 8; ++bit) {
				if ((remainder & 0x01) != 0) {
					remainder = (remainder >>> 1) ^ polynomial;
				} else {
					remainder >>>= 1;
				}
			}
			crcTable[dividend] = (short) remainder;
		}
	}
}
