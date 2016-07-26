import java.io.*;
import java.net.*;
import java.util.*;
import java.nio.*;
import java.util.zip.*;

public class FileReceiver {

	/** Protocol Constants **/
	// Initialise fragment and packet parameters
	static int maxPacketSize = 1000;
	static int fragSize = 20000000; //20MB
	static int checksumSize = 8;
	static int seqNumSize = 4;
	static int headerSize = checksumSize + seqNumSize;
	static int payloadSize = maxPacketSize - headerSize;
	/** End of Protocol Constants **/
	
	static DatagramSocket socket;
	static CRC32 crc = new CRC32();
	static String tempFileName = "temp";
	
	public static void main(String[] args) {
		socket = getSocket(Integer.parseInt(args[0]));
		if(socket == null) return;
		
		byte[] fragment = new byte[fragSize];
		ByteBuffer buffer = ByteBuffer.wrap(fragment);
		int fragNum = 0;
		int fragLen = 0;
		long totalRecv = 0;
		HashSet<Integer> storedSeq = new HashSet<Integer>();
		
		FileOutputStream file = getFileOutputStream(tempFileName);
		if(file == null) return;
		
		byte[] sequence = new byte[headerSize + payloadSize];
		DatagramPacket packet = new DatagramPacket(sequence, sequence.length);
		Integer seqNum, dataSize;
		boolean isComplete = false;
		while(true){
			try {
				socket.receive(packet);
				
				// Drop packet if corrupted or not from this fragment
				if((seqNum = getData(packet.getLength(), sequence, fragNum)) == null) continue;

				// Calculate the data size of this packet
				dataSize = packet.getLength() - headerSize;
				
				// Terminating condition
				if(seqNum == -1){
					if (!isComplete){
						// Flush the final fragment
						file.write(fragment, 0, fragLen);
						file.flush();
						totalRecv += fragLen;
						// Rename file
						byte[] chars = new byte[dataSize];
						ByteBuffer string = ByteBuffer.wrap(chars);
						string.put(sequence, headerSize, dataSize);
						String filename = new String(chars);
						file.close();
						File oldFile = new File(tempFileName);
						File newFile = new File(filename);
						oldFile.renameTo(newFile);						
						isComplete = true;
						System.out.println("Fragment " + fragNum + " (" + fragLen + " bytes) received.");
						System.out.println("File transfer (total of " + totalRecv + " bytes) complete. Continuing to ack...");
					}
					// Ack
					sendAck(seqNum, packet.getSocketAddress());
					continue;
				}
				
				// Store in fragment if not already stored
				if(!storedSeq.contains(seqNum)){
					buffer.position(seqNum % fragSize);
					buffer.put(sequence, headerSize, dataSize);
					storedSeq.add(seqNum);
					fragLen += dataSize;
					
					// Write fragment to file
					if(fragLen == fragSize){
						file.write(fragment);
						file.flush();
						System.out.println("Fragment " + fragNum + " (" + fragLen + " bytes) received. Waiting for fragment " + (fragNum + 1));
						fragNum++;
						totalRecv += fragLen;
						fragLen = 0;
					}
				}
				
				// Ack
				sendAck(seqNum, packet.getSocketAddress());
				
			} catch (IOException e) {
				e.printStackTrace();
				continue;
			}
		}
	}
	
	private static void sendAck(int seqNum, SocketAddress socketAddress){
		byte[] packet = new byte[checksumSize + seqNumSize];
		ByteBuffer buffer = ByteBuffer.wrap(packet);
		buffer.putLong(0);
		buffer.putInt(seqNum);
		crc.reset();
		crc.update(packet, checksumSize, packet.length-checksumSize);
		buffer.rewind();
		buffer.putLong(crc.getValue());
		try {
			socket.send(new DatagramPacket(packet, packet.length, socketAddress));
		} catch (IOException e) {
			e.printStackTrace();
		}
	}
	
	private static Integer getData(int packetLength, byte[] response, int fragNum) {
		if(packetLength < checksumSize + seqNumSize) return null;
		ByteBuffer buffer = ByteBuffer.wrap(response);
		long checksum = buffer.getLong();
		crc.reset();
		crc.update(response, checksumSize, packetLength-checksumSize);
		if(checksum != crc.getValue()) return null;
		int seqNum = buffer.getInt();
		int upperBound = (fragNum + 1) * fragSize;
		if(seqNum >= upperBound) return null;
		return seqNum;
	}
	
	private static FileOutputStream getFileOutputStream(String filename) {
		try {
			return new FileOutputStream(filename);
		} catch (IOException e) {
			System.out.println("Error: Failed to open inputstream to file: " + filename);
			e.printStackTrace();
		}		
		return null;
	}
	
	private static DatagramSocket getSocket(int port){
		try {
			return new DatagramSocket(port);
		} catch (SocketException e) {
			System.out.println("Error: Failed to open socket.");
			e.printStackTrace();
		}
		return null;
	}

}
