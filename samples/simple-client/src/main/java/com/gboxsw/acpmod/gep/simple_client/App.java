package com.gboxsw.acpmod.gep.simple_client;

import java.util.Scanner;

import com.gboxsw.acpmod.gep.*;

/**
 * Simple demo of GEPMessenger.
 */
public class App {

	public static void main(String[] args) {
		// create socket
		SerialPortSocket socket = new SerialPortSocket("COM23", 9600);

		// identification of the client (used only for receiving messages, 0
		// denotes that all messages should be received)
		int clientId = 0;
		// maximal length of received messages
		int maxMessageLength = 100;

		// create messenger with listener for receiving messages.
		GEPMessenger messenger = new GEPMessenger(socket, clientId, maxMessageLength,
				new GEPMessenger.MessageListener() {

					public void onMessageReceived(int tag, byte[] message) {
						if (tag >= 0) {
							System.out.println("Message received (tag " + tag + "): ");
						} else {
							System.out.println("Message received: ");
						}
						System.out.println(new String(message));
					}

				});

		// start messenger.
		System.out.println("Starting messenger...");
		messenger.start(true);
		if (!messenger.isConnected()) {
			System.out.println("Unable to open connection.");
			return;
		}

		System.out.println("Messenger is started.");

		// send lines as messages to the messenger.
		System.out.println();
		System.out.println("Each line defines a message to send.\nFormat: [id of receiver] [message content]");
		try (Scanner s = new Scanner(System.in)) {
			// tag counter - additional information that can be attached to
			// message
			int tagCounter = 1;
			while (s.hasNextLine()) {
				String line = s.nextLine();
				try (Scanner lineScanner = new Scanner(line)) {
					messenger.sendMessage(lineScanner.nextInt(), lineScanner.next().getBytes(), tagCounter);
					tagCounter++;
				}
			}
		}
	}
}
