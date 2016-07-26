import java.io.*;
import java.net.*;
import java.nio.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.zip.CRC32;


public class FileSender {

	/** Protocol Constants **/
	// Initialise fragment and packet parameters
	static int maxPacketSize = 1000;
	static int fragSize = 20000000; //20MB
	static int checksumSize = 8;
	static int seqNumSize = 4;
	static int headerSize = checksumSize + seqNumSize;
	static int payloadSize = maxPacketSize - headerSize;
	/** End of Protocol Constants **/
	
	public static void main(String[] args) {
		// Initialise input parameters
		String recvName = args[0];
		int recvPort = Integer.parseInt(args[1]);
		String srcFileName = args[2];
		String dstFileName = args[3];
		
		// Get inputstream of given file, quit if error occurs
		FileInputStream file = getFileInputStream(srcFileName);
		if(file == null) return;
		
		// Open socket for sending/receiving, quit if fail to get socket
		InetSocketAddress recvAddr = new InetSocketAddress(recvName, recvPort);
		final DatagramSocket socket = getSocket();
		if(socket == null) return;
		
		// Initialise data sets
		byte[] fragment = new byte[fragSize];
		int fragNum = 0;
		long totalSent = 0;
		final Set<Integer> unackedSeq = Collections.newSetFromMap(new ConcurrentHashMap<Integer, Boolean>());
		
		// Create a thread to listen on the UDP port
		createListener(unackedSeq, socket);
		
		// Fragment the file into byte arrays that fits into RAM and send each file fragment
		for(int fragLen = getFragment(file, fragment); fragLen != -1; fragLen = getFragment(file, fragment)){
			
			System.out.println("Sending fragment " + fragNum + " of size " + fragLen + " bytes");
			
			// Calculate the total number of sequences in this fragment
			int totalSeq = (int) Math.ceil((float)fragLen / (float)payloadSize);
			// Calculate the size of the final sequence in this fragment
			int finalSeqSize = (fragLen % payloadSize) == 0 ? payloadSize : fragLen % payloadSize;			

			// Each packet contains the checksum, sequence number and 
			// payload (initialised to max size)
			byte[] packet = new byte[headerSize + payloadSize];
			ByteBuffer buffer = ByteBuffer.wrap(packet);
			
			// Initialise a new checksum object
			CRC32 checksum = new CRC32();
			
			// Initialise the set of un-acked sequence numbers
			for(int i = 0; i < totalSeq; i++) {
				unackedSeq.add(fragNum * fragSize + i * payloadSize);
			}
			
			Integer[] unackedSeqNums = new Integer[totalSeq];
			// Ensures that all segments of the fragment is received
			while(!unackedSeq.isEmpty()){
				// Copy un-acked sequence numbers to an array and (re)send them
				unackedSeq.toArray(unackedSeqNums);
				for(Integer globalSeqNum : unackedSeqNums) {
					if(globalSeqNum == null) break;
					int localSeqNum = globalSeqNum % fragSize;
					// Set the size of this sequence
					int seqSize = (localSeqNum == (totalSeq-1)*payloadSize) ? finalSeqSize : payloadSize;
					// Set the size of this packet
					int packetSize = checksumSize + seqNumSize + seqSize;
					
					// Reserve 8 bytes for checksum
					buffer.putLong(0);
					// Indicate the global sequence number
					buffer.putInt(globalSeqNum);
					// Copy the payload
					buffer.put(fragment, localSeqNum, seqSize);
					// Set the checksum
					checksum.reset();
					checksum.update(packet, checksumSize, packetSize-checksumSize);
					buffer.rewind();
					buffer.putLong(checksum.getValue());
					
					// Send the packet and clear the buffer
					try {
						socket.send(new DatagramPacket(packet, packetSize, recvAddr));
					} catch (IOException e) {
						System.out.println("Error: Packet failed to send.");
						e.printStackTrace();
					}
					buffer.clear();
				}
								
				// Sleep for a short period of time to synchronize all the acks
				sleep(1);
			}

			totalSent += fragLen;
			fragNum++;
		}
		
		System.out.println("Total of " + totalSent + " bytes sent");
		
		// The terminating packet is the final unacked packet
		unackedSeq.add(-1);
		
		// Setup the terminating packet (seqNum == -1) with file name as payload
		byte[] packet = new byte[checksumSize + seqNumSize + dstFileName.length()];
		ByteBuffer buffer = ByteBuffer.wrap(packet);
		buffer.putLong(0);
		buffer.putInt(-1);
		buffer.put(dstFileName.getBytes());
		CRC32 checksum = new CRC32();
		checksum.reset();
		checksum.update(packet, checksumSize, packet.length-checksumSize);
		buffer.rewind();
		buffer.putLong(checksum.getValue());
		System.out.println("Requesting to terminate connection...");
		while(!unackedSeq.isEmpty()){
			try {
				socket.send(new DatagramPacket(packet, packet.length, recvAddr));
			} catch (IOException e) {
				e.printStackTrace();
				continue;
			}			
		}
		
		System.out.println("File transfer completed.");
		System.exit(0);
	}
	
	private static void createListener(Set<Integer> unackedSeq, DatagramSocket socket) {
		// Spawn off a new thread to listen for acks
		new Thread(){
			private CRC32 crc = new CRC32();
			public void run(){
				byte[] response = new byte[headerSize];
				DatagramPacket packet = new DatagramPacket(response, response.length);
				Integer seqNum;
				while(true){
					try {
						socket.receive(packet);
						if((seqNum = getData(packet.getLength(), response)) == null) continue;
						unackedSeq.remove(seqNum);
					} catch (IOException e) {
						e.printStackTrace();
						continue;
					}
				}
			}
			private Integer getData(int packetLength, byte[] response) {
				if(packetLength != headerSize) return null;
				ByteBuffer buffer = ByteBuffer.wrap(response);
				long checksum = buffer.getLong();
				crc.reset();
				crc.update(response, checksumSize, packetLength-checksumSize);
				if(checksum != crc.getValue()) return null;
				return buffer.getInt();
			}
		}.start();
	}

	private static void sleep(int i) {
		try {
			Thread.sleep(i);
		} catch (InterruptedException e) {
			e.printStackTrace();
		}
	}

	private static int getFragment(FileInputStream inputStream, byte[] array){
		try {
			return inputStream.read(array);
		} catch (IOException e) {
			e.printStackTrace();
		}
		return -1;
	}
	
	private static FileInputStream getFileInputStream(String filename){
		try {
			return new FileInputStream(filename);
		} catch (FileNotFoundException e) {
			System.out.println("Error: " + filename + " not found.");
			e.printStackTrace();
		}
		return null;
	}
	
	private static DatagramSocket getSocket(){
		try {
			return new DatagramSocket();
		} catch (SocketException e) {
			System.out.println("Error: Unable to open a socket.");
			e.printStackTrace();
		}
		return null;
	}
}
