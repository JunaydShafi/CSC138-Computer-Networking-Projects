import java.io.*;
import java.net.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.*;

public class receiver {

    private final ConcurrentHashMap<String, Transfer> transfers = new ConcurrentHashMap<>();
    private final Set<String> completed = ConcurrentHashMap.newKeySet();
    private final Object outputLock = new Object();

    private final DatagramSocket socket;
    private final boolean verbose;

    private static final long TIMEOUT_MS = 30_000;

    private class Transfer {
        final String transferId;
        final String filename;
        final int totalChunks;
        final String expectedChecksum;
        final byte[][] chunks;
        final boolean[] received;
        int receivedCount;
        long lastActivity;

        Transfer(String transferId, String filename, //professor provided code
                 int totalChunks, String checksum) {
            this.transferId = transferId;
            this.filename = filename;
            this.totalChunks = totalChunks;
            this.expectedChecksum = checksum;
            this.chunks = new byte[totalChunks][];
            this.received = new boolean[totalChunks];
            this.receivedCount = 0;
            this.lastActivity = System.currentTimeMillis();
        }

        /** Store a chunk.  Returns false if seq is out of range or a dup. */
        synchronized boolean storeChunk(int seq, byte[] data) {
            lastActivity = System.currentTimeMillis();
            if (seq < 0 || seq >= totalChunks) return false;
            if (received[seq]) return false;
            chunks[seq] = data;
            received[seq] = true;
            receivedCount++;
            return true;
        }

        /** True if this seqnum has already been stored. */
        synchronized boolean isDuplicate(int seq) {
            if (seq < 0 || seq >= totalChunks) return false;
            return received[seq];
        }

        /** True if every chunk has been received. */
        synchronized boolean isComplete() {
            return receivedCount == totalChunks;
        }

        /** Update the last-activity timestamp (e.g., on END). */
        synchronized void touch() {
            lastActivity = System.currentTimeMillis();
        }

        /** True if no packet has arrived for more than TIMEOUT_MS. */
        synchronized boolean isStale(long now) {
            return now - lastActivity > TIMEOUT_MS;
        }

        /**
         * Concatenate all chunks in order and compute the MD5 hex digest.
         * Compare the result to expectedChecksum yourself after calling this.
         */
        synchronized String reassembleAndVerify() throws Exception {
            ByteArrayOutputStream bos = new ByteArrayOutputStream();
            for (int i = 0; i < totalChunks; i++) {
                bos.write(chunks[i]);
            }
            byte[] fileBytes = bos.toByteArray();
            MessageDigest md = MessageDigest.getInstance("MD5");
            byte[] digest = md.digest(fileBytes);
            StringBuilder hex = new StringBuilder();
            for (byte b : digest) hex.append(String.format("%02x", b));
            return hex.toString();
        }
    }

    public receiver(int port, boolean verbose) throws SocketException {// professor provided code
        this.socket = new DatagramSocket(port);
        this.verbose = verbose;
    }

    public void run() throws IOException             // main loop for the UDP server
    {
        System.err.println("[receiver] listening...");
        startCleanupThread();//make the backround thread to get rid of timed out transfers

        byte[] buf = new byte[65535]; // the buffer for incoming UDP

        while (true) //keep looping to listen for packets
        {
            try 
            {
                DatagramPacket pkt = new DatagramPacket(buf, buf.length);
                socket.receive(pkt); //  block until UDP is recieved

                String msg = new String(pkt.getData(), 0, pkt.getLength()).trim();
                InetAddress addr = pkt.getAddress(); // getting sender info and the message
                int port = pkt.getPort();

                String[] tokens = msg.split(" ", 4); // splitting the message to preserve the payload

                switch (tokens[0]) // depending what message type use switch to handle them
                 {
                    case "BEGIN":
                        handleBegin(addr, port, tokens);
                        break;
                    case "DATA":
                        handleData(addr, port, tokens);
                        break;
                    case "END":
                        handleEnd(addr, port, tokens);
                        break;
                    default:
                        sendReply(addr, port, "ERR malformed");
                }

            } 
            catch (Exception ignored) // bad packets
            {
            }
        }
    }

    private void handleBegin(InetAddress addr, int port, String[] tokens) throws IOException // handles the begin message and handles the file transfer
    {

        String[] parts = String.join(" ", tokens).split(" "); //use tokens to split string into parts

        if (parts.length != 5) // make sure the messages are 5 parts
        {
            sendReply(addr, port, "ERR malformed");
            return;
        }

        String transferId = parts[1];
        String filename = parts[2];//getting the parts of the split

        int totalChunks;// get the num of the chunks

        try 
        {
            totalChunks = Integer.parseInt(parts[3]);
        } 
        
        catch (Exception e) 
        {
            sendReply(addr, port, "ERR malformed");
            return;
        }

        String checksum = parts[4];

        if (transfers.containsKey(transferId)) //check for duplicated info
        {
            sendReply(addr, port, "ERR duplicate transfer");
            return;
        }

        Transfer t = new Transfer(transferId, filename, totalChunks, checksum);
        transfers.put(transferId, t);// creating transfer object and stors it 

        log("RECV %s BEGIN chunks=%d", transferId, totalChunks);
        sendReply(addr, port, "ACK " + transferId + " BEGIN");
    }

    private void handleData(InetAddress addr, int port, String[] tokens) throws Exception // func handles the data of the chunks, duplicates, and out of order
    {

        if (tokens.length != 4) //check size of tokens
        {
            sendReply(addr, port, "ERR malformed");
            return;
        }

        String transferId = tokens[1];
        Transfer t = transfers.get(transferId);// find the transfer state with thetransferID

        if (t == null) 
        {
            if (completed.contains(transferId)) 
            {
                sendReply(addr, port, "ACK " + transferId + " " + tokens[2]);
                return;
            }
            sendReply(addr, port, "ERR unknown transfer");
            return;
        }

        int seq;

        try 
        {
            seq = Integer.parseInt(tokens[2]);
        } 
        
        catch (Exception e) 
        {
            sendReply(addr, port, "ERR malformed");
            return;
        }

        if (t.isDuplicate(seq)) //check if this chunk is a duplicate
        {
            log("DUP %s seq=%d", transferId, seq);
            sendReply(addr, port, "ACK " + transferId + " " + seq);
            return;
        }

        byte[] data = Base64.getDecoder().decode(tokens[3]);//decode the payload

        if (!t.storeChunk(seq, data)) 
        {
            sendReply(addr, port, "ERR malformed");
            return;
        }

        log("RECV %s DATA seq=%d", transferId, seq);//log the new chunks
        sendReply(addr, port, "ACK " + transferId + " " + seq);

        if (t.isComplete()) 
        {
            completeTransfer(t);
        }
    }

    private void handleEnd(InetAddress addr, int port, String[] tokens) throws IOException // this func sends the messages when we are done transmitting and is the end message that checks if transfer can be completed
    {

        if (tokens.length != 2) 
        {
            sendReply(addr, port, "ERR malformed");
            return;
        }

        String transferId = tokens[1];
        Transfer t = transfers.get(transferId);

        if (t == null) 
        {
            if (completed.contains(transferId)) //checking if all the chunks are correct if so then send 
            {
                sendReply(addr, port, "ACK " + transferId + " END");
                return;
            }
            sendReply(addr, port, "ERR unknown transfer");
            return;
        }

        t.touch();
        sendReply(addr, port, "ACK " + transferId + " END");

        if (t.isComplete()) 
        {
            completeTransfer(t);
        }
    }

    private void completeTransfer(Transfer t) //reassemble file func
    {
        try 
        {
            String computed = t.reassembleAndVerify(); //reassemble the file and get checksome
            boolean ok = computed.equals(t.expectedChecksum);

            log("COMPLETE %s file=%s status=%s",
                t.transferId,
                t.filename,
                ok ? "OK" : "FAIL");//check if its same file using checksome

        } 
        
        catch (Exception e) //fail if not same ^
        {
            log("COMPLETE %s file=%s status=FAIL",
                t.transferId,
                t.filename);
        }

        completed.add(t.transferId);
        transfers.remove(t.transferId);//remove transfer when done
    }

    private void startCleanupThread() {// professor provided code
        Thread cleaner = new Thread(() -> {
            while (true) {
                try {
                    Thread.sleep(5000);
                    long now = System.currentTimeMillis();
                    for (Map.Entry<String, Transfer> e : transfers.entrySet()) {
                        if (e.getValue().isStale(now)) {
                            transfers.remove(e.getKey());
                            log("TIMEOUT %s", e.getKey());
                        }
                    }
                } catch (InterruptedException ex) {
                    break;
                }
            }
        });
        cleaner.setDaemon(true);
        cleaner.start();
    }

    /** Send a UDP reply datagram to the given address and port. */
    private void sendReply(InetAddress addr, int port, String message)
            throws IOException {
        byte[] data = message.getBytes();
        DatagramPacket pkt = new DatagramPacket(data, data.length, addr, port);
        socket.send(pkt);
        vlog("sent to %s:%d: %s", addr, port, message);
    }

    /** Print a line to stdout (thread-safe). */
    private void log(String fmt, Object... args) {
        synchronized (outputLock) {
            System.out.printf(fmt + "%n", args);
            System.out.flush();
        }
    }

    /** Print a line to stderr if verbose mode is on. */
    private void vlog(String fmt, Object... args) {
        if (verbose) System.err.printf("[receiver] " + fmt + "%n", args);
    }

    public static void main(String[] args) throws Exception {// professor provided code
        int ai = 0;
        boolean verbose = false;
        if (args.length > 0 && args[0].equals("-v")) {
            verbose = true;
            ai = 1;
        }

        if (args.length - ai != 1) {
            System.err.println("Usage: java receiver.java [-v] <port>");
            System.exit(1);
        }

        int port = Integer.parseInt(args[ai]);
        receiver r = new receiver(port, verbose);
        r.run();
    }
}