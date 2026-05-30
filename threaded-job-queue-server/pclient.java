import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

class PerfClient {
    public static void main(String[] args) throws Exception {
        PerfClient app = PerfClient.fromArgs(args);
        if (app == null) {
            return;
        }
        app.run();
    }

    private static PerfClient fromArgs(String[] args) {
        boolean verbose = false;
        int index = 0;

        if (args.length > 0 && "-v".equals(args[0])) {
            verbose = true;
            index++;
        }

        if (args.length - index < 5) {
            System.err.println("Usage: pclient.java [-v] <host> <port> <clients> <jobsPerClient> <ms>");
            return null;
        }

        String host = args[index];
        int port = Integer.parseInt(args[index + 1]);
        int clients = Integer.parseInt(args[index + 2]);
        int jobsPerClient = Integer.parseInt(args[index + 3]);
        int durationMs = Integer.parseInt(args[index + 4]);

        return new PerfClient(host, port, clients, jobsPerClient, durationMs, verbose);
    }

    private final String host;
    private final int port;
    private final int clients;
    private final int jobsPerClient;
    private final int durationMs;
    private final boolean verbose;

    private PerfClient(String host, int port, int clients, int jobsPerClient, int durationMs, boolean verbose) {
        this.host = host;
        this.port = port;
        this.clients = clients;
        this.jobsPerClient = jobsPerClient;
        this.durationMs = durationMs;
        this.verbose = verbose;
    }

    private void run() throws InterruptedException {
        List<Thread> threads = new ArrayList<>();
        for (int i = 0; i < clients; i++) {
            int clientId = i;
            Thread t = new Thread(() -> runClient(clientId));
            threads.add(t);
            t.start();
        }

        for (Thread t : threads) {
            t.join();
        }
    }

    private void runClient(int clientId) {
        try (Socket socket = new Socket(host, port);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true)) {

            if (verbose) {
                System.out.println("client " + clientId + " connected");
            }

            for (int i = 0; i < jobsPerClient; i++) {
                out.println("SUBMIT " + durationMs);
                String response = in.readLine();
                if (response == null) {
                    break;
                }
                if (verbose) {
                    System.out.println("client " + clientId + " -> " + response);
                }
            }
        } catch (Exception e) {
            System.err.println("Client " + clientId + " failed: " + e.getMessage());
        }
    }
}
