package com.gboxsw.acpmod.gep;

import java.io.*;
import jssc.SerialPort;
import jssc.SerialPortException;

import com.gboxsw.acpmod.gep.GEPMessenger.FullDuplexStream;
import com.gboxsw.acpmod.gep.GEPMessenger.FullDuplexStreamSocket;

/**
 * Stream socket creating full-duplex streams over a serial port.
 */
public class SerialPortSocket implements FullDuplexStreamSocket {

	/**
	 * Wrapper of a serial port providing input and output data. This socket is
	 * for single use, i.e., it cannot be reopened.
	 */
	private static final class SerialPortWrapper {
		/**
		 * Underlying serial port.
		 */
		final SerialPort serialPort;

		/**
		 * Nanoseconds to sleep when no data are available.
		 */
		final int nanosSleep;

		/**
		 * Milliseconds to sleep when no data are available.
		 */
		final long millisSleep;

		/**
		 * Bytes buffered from the input.
		 */
		byte[] bufferedInputBytes;

		/**
		 * Index of first buffered byte that has not been read.
		 */
		int inputReadIdx = 0;

		/**
		 * Buffer for a single output byte.
		 */
		byte[] singleByteBuffer = new byte[1];

		/**
		 * Indicates that port is requested to close. Port is requested to close
		 * if any exception is thrown by wrapped methods.
		 */
		volatile boolean closeRequested = false;

		/**
		 * Indicates that the port has been closed.
		 */
		boolean closed = false;

		/**
		 * Synchronization lock for accessing the wrapped serial port.
		 */
		final Object serialPortLock = new Object();

		/**
		 * Constructs a wrapped serial port and tries to open it.
		 * 
		 * @param portName
		 *            the identifier of the serial port.
		 * @param baudRate
		 *            the baud rate of serial connection.
		 * @throws SerialPortException
		 *             when opening of port failed.
		 */
		SerialPortWrapper(String portName, int baudRate) throws SerialPortException {
			serialPort = new SerialPort(portName);

			// open port
			serialPort.openPort();
			serialPort.setParams(baudRate, SerialPort.DATABITS_8, SerialPort.STOPBITS_1, SerialPort.PARITY_NONE);

			// compute sleep interval with respect to baud rate.
			long nanosPerByte = Math.max((1_000_000_000L / baudRate) * 8, 100);
			nanosSleep = (int) (nanosPerByte % 1_000_000);
			millisSleep = nanosPerByte / 1_000_000;
		}

		/**
		 * Ensures that some input bytes are available for reading.
		 * Synchronization must be ensured by the caller.
		 */
		void waitForData() {
			if (bufferedInputBytes != null) {
				return;
			}

			while (!closeRequested) {
				byte[] data;
				synchronized (serialPortLock) {
					if (closeRequested) {
						break;
					}

					try {
						data = serialPort.readBytes();
					} catch (Exception e) {
						closeRequested = true;
						break;
					}
				}

				if ((data != null) && (data.length > 0)) {
					bufferedInputBytes = data;
					inputReadIdx = 0;
					return;
				}

				try {
					Thread.sleep(millisSleep, nanosSleep);
				} catch (Exception ignore) {

				}
			}
		}

		/**
		 * Return number of input bytes buffered in serial port.
		 * 
		 * @return the number of buffered bytes.
		 */
		int getAvailableBytes() {
			synchronized (serialPortLock) {
				if (closeRequested) {
					return -1;
				}

				try {
					return serialPort.getInputBufferBytesCount();
				} catch (Exception e) {
					closeRequested = true;
				}

				return -1;
			}
		}

		/**
		 * Closes the underlying (wrapped) serial port.
		 */
		void close() {
			closeRequested = true;
			synchronized (serialPortLock) {
				try {
					if (!closed) {
						closed = true;
						serialPort.closePort();
					}
				} catch (Exception ignore) {
					// silent closing
				}
			}
		}

		/**
		 * Writes a single byte to output of wrapped serial port.
		 * 
		 * @param b
		 *            byte to be written.
		 * @return true, if the byte has been written, false otherwise.
		 */
		boolean write(int b) {
			synchronized (serialPortLock) {
				try {
					singleByteBuffer[0] = (byte) b;
					serialPort.writeBytes(singleByteBuffer);
					return true;
				} catch (Exception e) {
					closeRequested = true;
					return false;
				}
			}
		}
	}

	/**
	 * Input stream for reading data from a serial port.
	 */
	private static final class SerialInputStream extends InputStream {

		/**
		 * Wrapper of a serial port.
		 */
		private final SerialPortWrapper portWrapper;

		/**
		 * Internal serialization lock.
		 */
		private final Object readOperationLock = new Object();

		/**
		 * Constructs input stream for a wrapped open serial port.
		 * 
		 * @param serialPortWrapper
		 *            the wrapper of a serial port.
		 */
		public SerialInputStream(SerialPortWrapper serialPortWrapper) {
			this.portWrapper = serialPortWrapper;
		}

		@Override
		public int read() throws IOException {
			synchronized (readOperationLock) {
				portWrapper.waitForData();
				if (portWrapper.closeRequested) {
					return -1;
				}

				int result = portWrapper.bufferedInputBytes[portWrapper.inputReadIdx];
				portWrapper.inputReadIdx++;
				if (portWrapper.inputReadIdx >= portWrapper.bufferedInputBytes.length) {
					portWrapper.bufferedInputBytes = null;
				}

				return result;
			}
		}

		@Override
		public int read(byte[] b, int off, int len) throws IOException {
			if (len <= 0) {
				return 0;
			}

			synchronized (readOperationLock) {
				portWrapper.waitForData();
				if (portWrapper.closeRequested) {
					return -1;
				}

				int bytesToRead = portWrapper.bufferedInputBytes.length - portWrapper.inputReadIdx;
				bytesToRead = Math.min(bytesToRead, len);
				System.arraycopy(portWrapper.bufferedInputBytes, portWrapper.inputReadIdx, b, off, bytesToRead);
				portWrapper.inputReadIdx += bytesToRead;
				if (portWrapper.inputReadIdx >= portWrapper.bufferedInputBytes.length) {
					portWrapper.bufferedInputBytes = null;
				}

				return bytesToRead;
			}
		}

		@Override
		public int available() throws IOException {
			synchronized (readOperationLock) {
				if (portWrapper.closeRequested) {
					return 0;
				}

				if (portWrapper.bufferedInputBytes != null) {
					return portWrapper.bufferedInputBytes.length - portWrapper.inputReadIdx;
				} else {
					return Math.max(portWrapper.getAvailableBytes(), 0);
				}
			}
		}

		@Override
		public void close() throws IOException {
			portWrapper.closeRequested = true;
		}
	}

	/**
	 * Output stream for writing data to a serial port.
	 */
	private static final class SerialOutputStream extends OutputStream {

		/**
		 * Wrapper of a serial port.
		 */
		private final SerialPortWrapper portWrapper;

		/**
		 * Constructs output stream for a wrapped open serial port.
		 * 
		 * @param serialPortWrapper
		 *            the wrapper of a serial port.
		 */
		public SerialOutputStream(SerialPortWrapper serialPortWrapper) {
			this.portWrapper = serialPortWrapper;
		}

		@Override
		public void write(int b) throws IOException {
			if (!portWrapper.write(b)) {
				throw new IOException("Failed to write to a serial port.");
			}
		}

		@Override
		public void close() throws IOException {
			portWrapper.closeRequested = true;
		}
	}

	/**
	 * Implementation of full-duplex stream over a serial port.
	 */
	private static final class SerialFullDuplexStream implements FullDuplexStream {

		/**
		 * Wrapped serial port with active communication.
		 */
		private final SerialPortWrapper serialPortWrapper;

		/**
		 * Input stream of the open communication over a serial port.
		 */
		private final SerialInputStream serialInputStream;

		/**
		 * Input stream of the open communication over a serial port.
		 */
		private final SerialOutputStream serialOutputStream;

		/**
		 * Constructs a full-duplex stream over a serial port.
		 * 
		 * @param serialPortWrapper
		 *            the wrapped serial port which is already open.
		 */
		private SerialFullDuplexStream(SerialPortWrapper serialPortWrapper) {
			this.serialPortWrapper = serialPortWrapper;
			serialInputStream = new SerialInputStream(serialPortWrapper);
			serialOutputStream = new SerialOutputStream(serialPortWrapper);
		}

		@Override
		public InputStream getInputStream() {
			return serialInputStream;
		}

		@Override
		public OutputStream getOutputStream() {
			return serialOutputStream;
		}

		@Override
		public void close() {
			serialPortWrapper.close();
		}
	}

	/**
	 * Identifier of the serial port.
	 */
	private final String portName;

	/**
	 * Baud rate of serial connection.
	 */
	private final int baudRate;

	/**
	 * Constructs the stream socket to a serial port.
	 * 
	 * @param portName
	 *            the identifier of the serial port.
	 * @param baudRate
	 *            the baud rate of serial connection.
	 */
	public SerialPortSocket(String portName, int baudRate) {
		this.portName = portName;
		this.baudRate = Math.abs(baudRate);
	}

	@Override
	public FullDuplexStream createStream() throws IOException {
		// create and open serial port
		SerialPortWrapper serialPortWrapper;
		try {
			serialPortWrapper = new SerialPortWrapper(portName, baudRate);
		} catch (Exception e) {
			throw new IOException("Unable to open and initialize serial port (" + portName + "@" + baudRate + ").", e);
		}

		return new SerialFullDuplexStream(serialPortWrapper);
	}
}
