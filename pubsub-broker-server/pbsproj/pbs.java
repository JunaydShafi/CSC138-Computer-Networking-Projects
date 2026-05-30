import java.io.*;
import java.net.*;
import java.nio.charset.StandardCharsets;
import java.util.*;
import java.util.concurrent.*;

//============================================================================
// pbs - Pub-Sub Broker Server
//
// An HTTP/1.1 pub-sub message broker.
//
// Endpoints:
//   POST /subscribe/<topic>       Subscribe client to a topic
//   POST /unsubscribe/<topic>     Unsubscribe client from a topic
//   POST /publish/<topic>         Publish message body to all subscribers
//   GET  /messages                Retrieve and clear pending messages
//   GET  /status                  List topics with subscriber counts
//
// Client identification: X-Client-ID header (required for all except /status).
//
// Usage: java pbs.java [-q] [port]
//
//============================================================================

public class pbs {
    private static final int DEFAULT_PORT = 8080;

    // Topic -> set of subscribed client IDs
    private final ConcurrentHashMap<String, Set<String>> topics = new ConcurrentHashMap<>();

    // Client ID -> queue of pending messages
    private final ConcurrentHashMap<String, Queue<String>> mailboxes = new ConcurrentHashMap<>();

    private boolean quiet = false;


    public static void main(String[] args) {
        new pbs().appMain(args);
    }

    public void appMain(String[] args) {
        int port = DEFAULT_PORT;

        for (int i = 0; i < args.length; i++) {
            if (args[i].equals("-q")) {
                quiet = true;
            } else {
                try {
                    port = Integer.parseInt(args[i]);
                } catch (NumberFormatException e) {
                    System.err.println("Invalid port, using default: " + DEFAULT_PORT);
                }
            }
        }

        try (ServerSocket serverSocket = new ServerSocket(port)) {
            System.out.println("PBS started on port " + port);

            while (true) {
                try {
                    Socket client = serverSocket.accept();
                    new Thread(() -> {
                        try {
                            handleConnection(client);
                        } catch (IOException e) {
                            // Client disconnected or error
                        } finally {
                            try { client.close(); } catch (IOException e) { }
                        }
                    }).start();
                } catch (IOException e) {
                    System.err.println("Accept error: " + e.getMessage());
                }
            }
        } catch (IOException e) {
            System.err.println("Could not start server: " + e.getMessage());
        }
    }


    private void handleConnection(Socket socket) throws IOException {

        BufferedReader in = new BufferedReader( //create a reader
        new InputStreamReader(socket.getInputStream())
        );
        OutputStream out = socket.getOutputStream(); //create a socket

        String requestLine = in.readLine(); //read in the line requests
        if (requestLine == null) return;

        String[] parts = requestLine.split(" ");// we want to be able to prosess get messages
        if (parts.length < 2) {
            sendError(out, 400, "Bad Request");
            return;
        }

        String method = parts[0];
        String path = parts[1];

        Map<String, String> headers = parseHeaders(in); //put the headders into a map

        String clientId = headers.get("x-client-id"); // get headders

        String body = "";
        int contentLength = 0;

        if (headers.containsKey("content-length")) { //read the request body if it has content length
            try {
                contentLength = Integer.parseInt(headers.get("content-length"));
            } catch (NumberFormatException e) {
                sendError(out, 400, "Invalid Content-Length");
                return;
            }
        }

        if (contentLength > 0) {
            char[] buf = new char[contentLength];
            int totalRead = 0;

            while (totalRead < contentLength) {
                int read = in.read(buf, totalRead, contentLength - totalRead);
                if (read == -1) break;
                totalRead += read;
            }

            body = new String(buf, 0, totalRead);
        }

        dispatchRequest(out, method, path, clientId, body); //call the next func to handle the request
    }

    private Map<String, String> parseHeaders(BufferedReader in) throws IOException {
        Map<String, String> headers = new HashMap<>();
        String line;
        while ((line = in.readLine()) != null && !line.isEmpty()) {
            int colon = line.indexOf(':');
            if (colon > 0) {
                headers.put(
                    line.substring(0, colon).trim().toLowerCase(),
                    line.substring(colon + 1).trim()
                );
            }
        }
        return headers;
    }

 
    private void dispatchRequest(OutputStream out, String method, String path, String clientId, String body) throws IOException 
    {
        //EXTRA CREDIT-----------------------------
        if (method.equals("OPTIONS")) { // Handle the CORS preflight request
            PrintWriter writer = new PrintWriter(out, false);
            writer.print("HTTP/1.1 204 No Content\r\n");
            writer.print("Access-Control-Allow-Origin: *\r\n");
            writer.print("Access-Control-Allow-Methods: GET, POST, OPTIONS\r\n");
            writer.print("Access-Control-Allow-Headers: X-Client-ID, Content-Type, Content-Length\r\n");
            writer.print("Access-Control-Max-Age: 86400\r\n");
            writer.print("Content-Length: 0\r\n");
            writer.print("Connection: close\r\n");
            writer.print("\r\n");
            writer.flush();
            return;
        }
        // END of Extra credit-------------

        if (method.equals("GET") && path.equals("/status")) { // process the get status message
            handleStatus(out);
            return;
        }

        if (method.equals("GET") && path.equals("/messages")) { //process the messages
            if (clientId == null) {
                sendError(out, 400, "Missing X-Client-ID");
                return;
            }
            handleMessages(out, clientId);
            return;
        }

        if (method.equals("POST") && path.startsWith("/subscribe/")) { //process the subscribe
            if (clientId == null) {
                sendError(out, 400, "Missing X-Client-ID");
                return;
            }
            String topic = path.substring("/subscribe/".length());
            handleSubscribe(out, clientId, topic);
            return;
        }

        if (method.equals("POST") && path.startsWith("/unsubscribe/")) {//process unsubscribe
            if (clientId == null) {
                sendError(out, 400, "Missing X-Client-ID");
                return;
            }
            String topic = path.substring("/unsubscribe/".length());
            handleUnsubscribe(out, clientId, topic);
            return;
        }

        if (method.equals("POST") && path.startsWith("/publish/")) {//process the publish
            if (clientId == null) {
                sendError(out, 400, "Missing X-Client-ID");
                return;
            }
            String topic = path.substring("/publish/".length());
            handlePublish(out, clientId, topic, body);
            return;
        }

        sendError(out, 404, "Not Found");//command not found error
    }

   private void handleSubscribe(OutputStream out, String clientId, String topic) throws IOException {

    topics.computeIfAbsent(topic, k -> ConcurrentHashMap.newKeySet())// add the client to the topic
          .add(clientId);

    mailboxes.computeIfAbsent(clientId, k -> new ConcurrentLinkedQueue<>()); //add to mailbox

    String json = "{\"status\":\"subscribed\",\"topic\":\"" + escapeJson(topic) + "\"}";
    byte[] body = json.getBytes(StandardCharsets.UTF_8);

    sendResponse(out, 201, "Created", "application/json", body);
}


   private void handleUnsubscribe(OutputStream out, String clientId, String topic) throws IOException {
    Set<String> subs = topics.get(topic);

    if (subs != null) { //remove the subscriber from the topics.
        subs.remove(clientId);
        if (subs.isEmpty()) {
            topics.remove(topic);
        }
    }

    String json = "{\"status\":\"unsubscribed\",\"topic\":\"" + escapeJson(topic) + "\"}";
    byte[] body = json.getBytes(StandardCharsets.UTF_8);

    sendResponse(out, 200, "OK", "application/json", body);
}

    
   private void handlePublish(OutputStream out, String clientId, String topic, String body) throws IOException {
    Set<String> subs = topics.get(topic);
    int delivered = 0;

    if (subs != null) { //add the message so all subscribers see it
        for (String sub : subs) {
            Queue<String> q = mailboxes.get(sub);
            if (q != null) {
                q.add(topic + ": " + body);
                delivered++;
            }
        }
    }

    String json = "{\"status\":\"published\",\"topic\":\"" + escapeJson(topic) + //build a json to send it over http
                  "\",\"delivered\":" + delivered + "}";
    byte[] resp = json.getBytes(StandardCharsets.UTF_8);

    sendResponse(out, 200, "OK", "application/json", resp);//use this  func to send the response message
}


   private void handleMessages(OutputStream out, String clientId) throws IOException {
    Queue<String> q = mailboxes.get(clientId); //use queue to store the mailboxes

    StringBuilder sb = new StringBuilder();
    sb.append("{\"messages\":[");

    int count = 0;
    boolean first = true;

    if (q != null) {//store the messages
        String msg;
        while ((msg = q.poll()) != null) {
            if (!first) sb.append(",");
            first = false;

            sb.append("\"").append(escapeJson(msg)).append("\"");
            count++;
        }
    }

    sb.append("],\"count\":").append(count).append("}");

    byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);

    sendResponse(out, 200, "OK", "application/json", body);
}


    private void handleStatus(OutputStream out) throws IOException {
       StringBuilder sb = new StringBuilder();
        sb.append("{\"topics\":{");

        boolean first = true;

        for (Map.Entry<String, Set<String>> entry : topics.entrySet()) {//append all the info
            if (!first) sb.append(",");
            first = false;

            sb.append("\"")
            .append(escapeJson(entry.getKey()))
            .append("\":")
            .append(entry.getValue().size());
        }

        sb.append("}}");

        byte[] body = sb.toString().getBytes(StandardCharsets.UTF_8);

        sendResponse(out, 200, "OK", "application/json", body);
    }


    private void sendResponse(OutputStream out, int code, String message,
                               String contentType, byte[] body) throws IOException {
        PrintWriter writer = new PrintWriter(out, false);

        writer.print("HTTP/1.1 " + code + " " + message + "\r\n");//print the status of the line

        writer.print("Content-Type: " + contentType + "\r\n");//print the headders of these contents
        writer.print("Content-Length: " + body.length + "\r\n");
        writer.print("Access-Control-Allow-Origin: *\r\n"); // <------- for the extra credit
        writer.print("Connection: close\r\n");

        writer.print("\r\n"); //the blank line /r/n that is required
        writer.flush();

        out.write(body); //writ ethe raw bytes
        out.flush();
    }

  
    private void sendError(OutputStream out, int code, String message) throws IOException {//send error func
        String json = "{\"error\":\"" + escapeJson(message) + "\"}";
        byte[] body = json.getBytes(StandardCharsets.UTF_8);

        sendResponse(out, code, message, "application/json", body);
    }

   

    private String escapeJson(String s) {
        return s.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n").replace("\r", "\\r");
    }
}
