package com.gboxsw.acpmod.gep;

import java.io.*;
import java.net.Socket;

import com.gboxsw.acpmod.gep.GEPMessenger.FullDuplexStream;
import com.gboxsw.acpmod.gep.GEPMessenger.FullDuplexStreamSocket;

/**
 * Stream socket creating full-duplex streams over a tcp connection. TCP socket
 * can be used to access remote serial port over a network utilizing the utility
 * "socat". 
 * 
 * Example of socat usage: 
 * socat TCP4-LISTEN:5555,reuseaddr,fork /dev/tty0,b38400,raw,echo=0
 */
public class TCPSocket implements FullDuplexStreamSocket {

	/**
	 * Full-duplex stream wrapping an open tcp socket.
	 */
	private static class TCPFullDuplexStream implements FullDuplexStream {

		/**
		 * Open TCP socket.
		 */
		private final Socket socket;

		/**
		 * Input stream.
		 */
		private final InputStream inputStream;

		/**
		 * Output stream.
		 */
		private final OutputStream outputStream;

		/**
		 * Constructs wrapper of a tcp socket.
		 * 
		 * @param socket
		 *            the wrapped socket.
		 * @throws IOException
		 *             when construction of I/O streams failed.
		 */
		private TCPFullDuplexStream(Socket socket) throws IOException {
			this.socket = socket;
			this.inputStream = new BufferedInputStream(socket.getInputStream());
			this.outputStream = new BufferedOutputStream(socket.getOutputStream());
		}

		@Override
		public InputStream getInputStream() {
			return inputStream;
		}

		@Override
		public OutputStream getOutputStream() {
			return outputStream;
		}

		@Override
		public void close() {
			try {
				socket.close();
			} catch (Exception ignore) {

			}
		}
	}

	/**
	 * Host name.
	 */
	private final String host;

	/**
	 * Port number.
	 */
	private final int port;

	/**
	 * Constructs the stream socket to a TCP port.
	 * 
	 * @param host
	 *            the host name, or null for the loopback address
	 * @param port
	 *            the port number.
	 */
	public TCPSocket(String host, int port) {
		this.host = host;
		this.port = port;
	}

	@Override
	public FullDuplexStream createStream() throws IOException {
		try {
			Socket socket = new Socket(host, port);
			return new TCPFullDuplexStream(socket);
		} catch (Exception e) {
			throw new IOException("Unable to create and open connection to " + host + ":" + port + ".", e);
		}
	}

}
