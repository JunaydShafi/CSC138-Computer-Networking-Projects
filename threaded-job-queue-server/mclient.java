import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.Socket;

class ManualClient {
    public static void main(String[] args) throws Exception {
        if (args.length < 2) {
            System.err.println("Usage: mclient.java <host> <port>");
            return;
        }

        String host = args[0];
        int port = Integer.parseInt(args[1]);

        try (Socket socket = new Socket(host, port);
             BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
             PrintWriter out = new PrintWriter(socket.getOutputStream(), true);
             BufferedReader console = new BufferedReader(new InputStreamReader(System.in))) {

            String line;
            while (true) {
                System.out.println("Commands: SUBMIT <ms> | STATUS <id> | CANCEL <id> | QUIT");
                line = console.readLine();
                if (line == null) {
                    break;
                }
                out.println(line);
                String response = in.readLine();
                if (response == null) {
                    break;
                }
                System.out.println(response);
                if ("BYE".equals(response)) {
                    break;
                }
            }
        }
    }
}
