import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;

//============================================================================
// pbc - Pub-Sub Broker Client (Test Tool)
//
// Tests a PBS server by running a configurable scenario: multiple publishers
// and subscribers interact via topics, then results are verified.
//
// Usage: java pbc.java [options]
//   -h, --host <host>      Server host (default: localhost)
//   -p, --port <port>      Server port (default: 8080)
//   -t, --topics <n>       Number of topics (default: 3)
//   -s, --subscribers <n>  Subscribers per topic (default: 2)
//   -m, --messages <n>     Messages per topic (default: 5)
//   -d, --delay <ms>       Delay between publishes in ms (default: 50)
//   --help                 Show usage
//
// Test flow:
//   1. Create subscribers and subscribe each to assigned topics
//   2. Publish messages to each topic
//   3. Wait, then poll each subscriber for messages
//   4. Verify every message was delivered to every subscriber of its topic
//
//============================================================================

public class pbc {
    private String host = "localhost";
    private int port = 8080;
    private int numTopics = 3;
    private int subsPerTopic = 2;
    private int msgsPerTopic = 5;
    private int delayMs = 50;

    private AtomicInteger passed = new AtomicInteger(0);
    private AtomicInteger failed = new AtomicInteger(0);

    public static void main(String[] args) {
        new pbc().appMain(args);
    }

    public void appMain(String[] args) {
        parseArgs(args);

        System.out.println("PBC Test Configuration:");
        System.out.println("  Server: " + host + ":" + port);
        System.out.println("  Topics: " + numTopics);
        System.out.println("  Subscribers per topic: " + subsPerTopic);
        System.out.println("  Messages per topic: " + msgsPerTopic);
        System.out.println();

        try {
            runTest();
        } catch (Exception e) {
            System.err.println("Test failed: " + e.getMessage());
            e.printStackTrace();
            System.exit(1);
        }
    }

    private void parseArgs(String[] args) {
        for (int i = 0; i < args.length; i++) {
            switch (args[i]) {
                case "-h": case "--host":
                    if (i + 1 < args.length) host = args[++i];
                    break;
                case "-p": case "--port":
                    if (i + 1 < args.length) port = Integer.parseInt(args[++i]);
                    break;
                case "-t": case "--topics":
                    if (i + 1 < args.length) numTopics = Integer.parseInt(args[++i]);
                    break;
                case "-s": case "--subscribers":
                    if (i + 1 < args.length) subsPerTopic = Integer.parseInt(args[++i]);
                    break;
                case "-m": case "--messages":
                    if (i + 1 < args.length) msgsPerTopic = Integer.parseInt(args[++i]);
                    break;
                case "-d": case "--delay":
                    if (i + 1 < args.length) delayMs = Integer.parseInt(args[++i]);
                    break;
                case "--help":
                    printUsage();
                    System.exit(0);
                    break;
            }
        }
    }

    private void printUsage() {
        System.out.println("Usage: java pbc.java [options]");
        System.out.println("  -h, --host <host>       Server host (default: localhost)");
        System.out.println("  -p, --port <port>       Server port (default: 8080)");
        System.out.println("  -t, --topics <n>        Number of topics (default: 3)");
        System.out.println("  -s, --subscribers <n>   Subscribers per topic (default: 2)");
        System.out.println("  -m, --messages <n>      Messages per topic (default: 5)");
        System.out.println("  -d, --delay <ms>        Delay between publishes (default: 50)");
        System.out.println("  --help                  Show this help");
    }

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Test Execution
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    private void runTest() throws Exception {
        // Build topic names
        String[] topicNames = new String[numTopics];
        for (int i = 0; i < numTopics; i++) {
            topicNames[i] = "topic-" + i;
        }

        // Build subscriber IDs: sub-<topic>-<n>
        // Map: clientId -> list of topics subscribed to
        Map<String, List<String>> clientTopics = new LinkedHashMap<>();
        for (int t = 0; t < numTopics; t++) {
            for (int s = 0; s < subsPerTopic; s++) {
                String clientId = "sub-" + t + "-" + s;
                clientTopics.computeIfAbsent(clientId, k -> new ArrayList<>()).add(topicNames[t]);
            }
        }

        // Also subscribe some clients to multiple topics for cross-topic testing
        if (numTopics >= 2 && subsPerTopic >= 1) {
            String multiClient = "sub-multi";
            List<String> multiTopics = new ArrayList<>();
            multiTopics.add(topicNames[0]);
            multiTopics.add(topicNames[1]);
            clientTopics.put(multiClient, multiTopics);
        }

        // Phase 1: Subscribe all clients
        System.out.println("Phase 1: Subscribing clients...");
        for (Map.Entry<String, List<String>> entry : clientTopics.entrySet()) {
            for (String topic : entry.getValue()) {
                String resp = httpPost("/subscribe/" + topic, entry.getKey(), "");
                check("subscribe " + entry.getKey() + " -> " + topic,
                      resp != null && resp.contains("\"subscribed\""));
            }
        }

        // Phase 2: Check status
        System.out.println("\nPhase 2: Checking status...");
        String status = httpGet("/status", null);
        check("status returns topics", status != null && status.contains("topic-0"));
        System.out.println("  Status: " + status);

        // Phase 3: Publish messages
        System.out.println("\nPhase 3: Publishing messages...");
        // Track what we expect each client to receive
        Map<String, Set<String>> expected = new HashMap<>();
        for (String clientId : clientTopics.keySet()) {
            expected.put(clientId, new HashSet<>());
        }

        for (int t = 0; t < numTopics; t++) {
            for (int m = 0; m < msgsPerTopic; m++) {
                String msg = "msg-" + t + "-" + m;
                String resp = httpPost("/publish/" + topicNames[t], "publisher", msg);
                check("publish " + msg + " to " + topicNames[t],
                      resp != null && resp.contains("\"published\""));

                // Record expected deliveries
                for (Map.Entry<String, List<String>> entry : clientTopics.entrySet()) {
                    if (entry.getValue().contains(topicNames[t])) {
                        expected.get(entry.getKey()).add(topicNames[t] + ": " + msg);
                    }
                }

                if (delayMs > 0) Thread.sleep(delayMs);
            }
        }

        // Phase 4: Poll and verify
        System.out.println("\nPhase 4: Polling and verifying...");
        // Small delay to let any queuing settle
        Thread.sleep(100);

        int totalExpected = 0;
        int totalReceived = 0;

        for (Map.Entry<String, Set<String>> entry : expected.entrySet()) {
            String clientId = entry.getKey();
            Set<String> exp = entry.getValue();

            String resp = httpGet("/messages", clientId);
            if (resp == null) {
                check("poll " + clientId, false);
                continue;
            }

            // Parse messages from JSON response
            List<String> received = parseMessages(resp);
            Set<String> receivedSet = new HashSet<>(received);

            totalExpected += exp.size();
            totalReceived += received.size();

            // Check all expected messages were received
            boolean allReceived = receivedSet.containsAll(exp);
            check("verify " + clientId + " (" + received.size() + "/" + exp.size() + " messages)",
                  allReceived);

            if (!allReceived) {
                Set<String> missing = new HashSet<>(exp);
                missing.removeAll(receivedSet);
                for (String m : missing) {
                    System.out.println("    MISSING: " + m);
                }
            }
        }

        // Phase 5: Verify poll clears mailbox
        System.out.println("\nPhase 5: Verifying mailbox cleared after poll...");
        String firstClient = clientTopics.keySet().iterator().next();
        String resp = httpGet("/messages", firstClient);
        List<String> secondPoll = parseMessages(resp);
        check("second poll returns empty", secondPoll.isEmpty());

        // Phase 6: Unsubscribe and verify no delivery
        System.out.println("\nPhase 6: Unsubscribe test...");
        String unsubClient = clientTopics.keySet().iterator().next();
        String unsubTopic = clientTopics.get(unsubClient).get(0);
        httpPost("/unsubscribe/" + unsubTopic, unsubClient, "");
        httpPost("/publish/" + unsubTopic, "publisher", "after-unsub");
        Thread.sleep(100);
        resp = httpGet("/messages", unsubClient);
        List<String> afterUnsub = parseMessages(resp);
        boolean noDelivery = true;
        for (String m : afterUnsub) {
            if (m.contains("after-unsub")) { noDelivery = false; break; }
        }
        check("no delivery after unsubscribe", noDelivery);

        // Summary
        System.out.println("\n==================");
        System.out.println("Results: " + passed.get() + " passed, " + failed.get() + " failed");
        System.out.println("Messages: " + totalReceived + "/" + totalExpected + " delivered");

        if (failed.get() > 0) {
            System.out.println("FAIL");
            System.exit(1);
        } else {
            System.out.println("ALL TESTS PASSED");
        }
    }

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // HTTP Client
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    private String httpGet(String path, String clientId) {
        try (Socket socket = new Socket(host, port)) {
            PrintWriter out = new PrintWriter(socket.getOutputStream(), false);
            out.print("GET " + path + " HTTP/1.1\r\n");
            out.print("Host: " + host + "\r\n");
            if (clientId != null) out.print("X-Client-ID: " + clientId + "\r\n");
            out.print("Connection: close\r\n");
            out.print("\r\n");
            out.flush();
            return readResponse(socket);
        } catch (IOException e) {
            System.err.println("  HTTP GET " + path + " failed: " + e.getMessage());
            return null;
        }
    }

    private String httpPost(String path, String clientId, String body) {
        try (Socket socket = new Socket(host, port)) {
            byte[] bodyBytes = body.getBytes(StandardCharsets.UTF_8);
            PrintWriter out = new PrintWriter(socket.getOutputStream(), false);
            out.print("POST " + path + " HTTP/1.1\r\n");
            out.print("Host: " + host + "\r\n");
            if (clientId != null) out.print("X-Client-ID: " + clientId + "\r\n");
            out.print("Content-Length: " + bodyBytes.length + "\r\n");
            out.print("Connection: close\r\n");
            out.print("\r\n");
            out.flush();
            socket.getOutputStream().write(bodyBytes);
            socket.getOutputStream().flush();
            return readResponse(socket);
        } catch (IOException e) {
            System.err.println("  HTTP POST " + path + " failed: " + e.getMessage());
            return null;
        }
    }

    private String readResponse(Socket socket) throws IOException {
        BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));

        // Read status line
        String statusLine = in.readLine();
        if (statusLine == null) return null;

        // Read headers, find content-length
        int contentLength = -1;
        String line;
        while ((line = in.readLine()) != null && !line.isEmpty()) {
            if (line.toLowerCase().startsWith("content-length:")) {
                contentLength = Integer.parseInt(line.substring(15).trim());
            }
        }

        // Read body
        if (contentLength > 0) {
            char[] buf = new char[contentLength];
            int totalRead = 0;
            while (totalRead < contentLength) {
                int read = in.read(buf, totalRead, contentLength - totalRead);
                if (read == -1) break;
                totalRead += read;
            }
            return new String(buf, 0, totalRead);
        }

        return "";
    }

    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
    // Utilities
    //~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

    private void check(String label, boolean condition) {
        if (condition) {
            System.out.println("  PASS: " + label);
            passed.incrementAndGet();
        } else {
            System.out.println("  FAIL: " + label);
            failed.incrementAndGet();
        }
    }

    // Simple JSON message array parser — extracts strings from {"messages":["...","..."]}
    private List<String> parseMessages(String json) {
        List<String> messages = new ArrayList<>();
        if (json == null) return messages;

        int arrStart = json.indexOf('[');
        int arrEnd = json.lastIndexOf(']');
        if (arrStart < 0 || arrEnd < 0 || arrEnd <= arrStart + 1) return messages;

        String inner = json.substring(arrStart + 1, arrEnd);
        // Parse quoted strings, handling escaped quotes
        int i = 0;
        while (i < inner.length()) {
            if (inner.charAt(i) == '"') {
                StringBuilder sb = new StringBuilder();
                i++; // skip opening quote
                while (i < inner.length()) {
                    if (inner.charAt(i) == '\\' && i + 1 < inner.length()) {
                        char next = inner.charAt(i + 1);
                        if (next == '"') { sb.append('"'); i += 2; }
                        else if (next == '\\') { sb.append('\\'); i += 2; }
                        else if (next == 'n') { sb.append('\n'); i += 2; }
                        else { sb.append(next); i += 2; }
                    } else if (inner.charAt(i) == '"') {
                        i++; // skip closing quote
                        break;
                    } else {
                        sb.append(inner.charAt(i));
                        i++;
                    }
                }
                messages.add(sb.toString());
            } else {
                i++;
            }
        }
        return messages;
    }
}
