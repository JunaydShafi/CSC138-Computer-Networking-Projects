import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.HashMap;
import java.util.Map;

public class server { // changed name from class SimpleServer
    public static void main(String[] args) throws Exception {
        ServerConfig config = ServerConfig.fromArgs(args);
        if (config == null) {
            return;
        }

        JobRegistry registry = new JobRegistry();
        try (ServerSocket serverSocket = new ServerSocket(config.port)) {
            while (true) {
                Socket socket = serverSocket.accept();
                Thread connection = new Thread(new ClientHandler(socket, registry, config));
                connection.start();
            }
        }
    }
}

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//  ClientHandler owns a single client connection. It should read one line at a
//  time, validate commands, and return exactly one response line for each
//  request. It is responsible for:
//  - Parsing the command and arguments.
//  - Enforcing usage errors and bad input errors.
//  - Delegating to the job registry to create, lookup, or cancel jobs.
//  - Returning the protocol strings (JOB, STATUS, CANCELLED, NOTCANCELLED, BYE).
//  - Keeping all protocol output deterministic and single-line.
//
class ClientHandler implements Runnable {
    private final Socket socket;
    private final JobRegistry registry;
    private final ServerConfig config;

    ClientHandler(Socket socket, JobRegistry registry, ServerConfig config) {
        this.socket = socket;
        this.registry = registry;
        this.config = config;
    }

    @Override
    public void run() 
    {
        try (//open input and output of socket
            BufferedReader in = new BufferedReader(new InputStreamReader(socket.getInputStream()));
            PrintWriter out = new PrintWriter(socket.getOutputStream(), true))
        {
            String line;
            while ((line = in.readLine()) != null)//read one line
            {
                if (config.verbose) //if verbose
                {
                    System.out.println("recv: " + line);
                }

                String response = handle(line);//get response

                if (config.verbose) //print send if verbose
                {
                    System.out.println("send: " + response);
                }

                out.println(response);//and we say bye bye
                if ("BYE".equals(response)) break;
            }
        } 
        
        catch (Exception e)  // if the client has an issue don't nuke the server like war thunder
        {
            if (config.verbose) 
            {
                System.out.println("handler error: " + e);
            }
        } 
        finally 
        {
            try { socket.close(); } catch (Exception ignored) {}//everything is closed
        }
    }


    private String handle(String line) 
    {
        if (line == null) return "ERR EMPTY";//if the client disconnected

        line = line.trim();
        if (line.isEmpty()) return "ERR EMPTY";//if they just hit enter

        String[] tok = line.split("\\s+");
        String cmd = tok[0];

        switch (cmd) //all different commands
        {
            case "QUIT":
                if (tok.length != 1) return usageError("QUIT", "");
                return "BYE";

            case "SUBMIT":
                if (tok.length != 2) return usageError("SUBMIT", "<ms>");
                return handleSubmit(tok[1]);

            case "STATUS":
                if (tok.length != 2) return usageError("STATUS", "<id>");
                return handleStatus(tok[1]);

            case "CANCEL":
                if (tok.length != 2) return usageError("CANCEL", "<id>");
                return handleCancel(tok[1]);

            default:
                return "ERR UNKNOWN_COMMAND";
        }
    }



   private String handleSubmit(String value) 
   {
        long ms;
        try 
        {
            ms = Long.parseLong(value);
        } 
        catch (NumberFormatException e) //if bad input
        {
            return badMsError();
        }
        if (ms <= 0) return badMsError();

        Job job = registry.createJob(ms);
        return "JOB " + job.id();
    }


   private String handleStatus(String value) 
   {
        Long id = parseId(value);
        if (id == null) return badIdError();//nothing entered

        Job job = registry.find(id);
        if (job == null) 
        {
            return "STATUS " + id + " UNKNOWN";
        }

        return "STATUS " + id + " " + job.state();
    }



    private String handleCancel(String value) 
    {
        Long id = parseId(value);
        if (id == null) return badIdError();

        Job job = registry.find(id);
        if (job == null) //if unknown
        {
            return "NOTCANCELLED " + id + " UNKNOWN";
        }

        boolean ok = job.cancel();
        if (ok) //if its still in process
        {
            return "CANCELLED " + id;
        } 
        else 
        {
            return "NOTCANCELLED " + id + " " + job.state();//if job done
        }
    }



    private Long parseId(String value) //assign id
    {
        long id;
        try 
        {
            id = Long.parseLong(value);
        }
        catch (NumberFormatException e)
        {
            return null;
        }
        if (id <= 0) return null;
        return id;
    }


   private String usageError(String command, String args)
    {
        if (args == null || args.isEmpty()) return "ERR USAGE " + command;
        return "ERR USAGE " + command + " " + args;
    }


    private String badIdError()
    {
        return "ERR BAD_ID";
    }

    private String badMsError() 
    {
        return "ERR BAD_MS";
    }

}

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//  ServerConfig parses command-line args and holds server settings. It should:
//  - Accept an optional "-v" for verbose logging.
//  - Require a <port> argument.
//  - Print usage and return null if args are invalid.
//  - Expose the parsed port and verbose flag.
//
class ServerConfig {
    final int port;
    final boolean verbose;

    private ServerConfig(int port, boolean verbose) {
        this.port = port;
        this.verbose = verbose;
    }

    static ServerConfig fromArgs(String[] args) {
        boolean verbose = false;
        String portString = null;

        if(args.length == 1)//if just the number like server.java 9000
        {
            portString = args[0];
        }
        else if(args.length == 2 && "-v".equals(args[0]))//if you want logs
        {
            verbose = true;
            portString = args[1];
        }
        else
        {
            System.out.println("usage: java server.java [-v] <port>");
            return null;
        }

        int port;
        try //try to make the port
        {
            port = Integer.parseInt(portString);
        } 
        catch (NumberFormatException e)
        {
            System.out.println("usage: java server.java [-v] <port>");
            return null;
        }

        if (port < 1 || port > 65535) 
        {
            System.out.println("usage: java server.java [-v] <port>");
            return null;
        }

        return new ServerConfig(port, verbose);
    }
}

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//  JobRegistry stores jobs and assigns unique ids. It should:
//  - Keep a shared map of id -> Job.
//  - Generate monotonically increasing ids (1, 2, 3, ...).
//  - Think about which methods need synchronization for thread safety.
//
class JobRegistry {
    private final Map<Long, Job> jobs = new HashMap<>();
    private long nextId = 1;

   Job createJob(long durationMs) //defualt constructer
   {
        Job job;
        synchronized (this) 
        {
            long id = nextId++;//if someone wants another job assign next num
            job = new Job(id, durationMs);
            jobs.put(id, job);
        }
        new Thread(new JobWorker(job)).start();// start work after storing it
        return job;
    }


    Job find(long id) 
    {
        synchronized (this) {
            return jobs.get(id);
        }
    }

}

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//  JobWorker performs the simulated job work on its own thread and updates
//  the Job state as it starts, completes, or is cancelled.
//
//  This class is provided, but you are expected to read it and understand how it
//  drives the Job lifecycle.
//
class JobWorker implements Runnable {
    private final Job job;

    JobWorker(Job job) {
        this.job = job;
    }

    @Override
    public void run() {
        job.markRunning();

        long remaining = job.durationMs();
        long chunk = 25;
        while (remaining > 0 && !job.isCancelled()) {
            long sleepFor = Math.min(chunk, remaining);
            sleepQuietly(sleepFor);
            remaining -= sleepFor;
        }

        if (job.isCancelled()) {
            job.markCancelled();
        } else {
            job.markDone();
        }
    }

    private void sleepQuietly(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException ignored) {
            Thread.currentThread().interrupt();
        }
    }
}

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//  JobState provides a small set of named states that describe a job's
//  lifecycle. Think about how JobState is used as work begins, completes,
//  or is cancelled.
//
enum JobState {
    QUEUED,
    RUNNING,
    DONE,
    CANCELLED
}

//~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
//  Job represents a unit of work. It should:
//  - Store id and duration.
//  - Track state transitions (QUEUED -> RUNNING -> DONE or CANCELLED).
//  - Track cancellation requests and report whether cancel succeeded.
//  - Decide which methods need synchronization for thread safety.
//
class Job {
    //local variables
    private final long id;
    private final long durationMs;
    private JobState state = JobState.QUEUED;
    private boolean cancelRequested = false;


   Job(long id, long durationMs) //defualt constructer
    {
        this.id = id;
        this.durationMs = durationMs;
    }

    long id() 
    {
        return id;
    }

    long durationMs() 
    {
        return durationMs;
    }

    synchronized JobState state() 
    {
        return state;
    }

    synchronized boolean isCancelled() 
    {
        return cancelRequested || state == JobState.CANCELLED;
    }

   synchronized void markRunning() 
   {
        if (state == JobState.QUEUED) 
        {
            if (cancelRequested) state = JobState.CANCELLED;
            else state = JobState.RUNNING;
        }
    }


   synchronized void markDone() 
    {
        if (state == JobState.RUNNING) state = JobState.DONE;
    }

    synchronized void markCancelled() 
    {
        state = JobState.CANCELLED;
    }

   synchronized boolean cancel() 
    {
        if (state == JobState.DONE || state == JobState.CANCELLED) //if job is running or already cancled dont cancel it again
        {
            return false;
        }

        cancelRequested = true;

        if (state == JobState.QUEUED) //if th ejob is about to run we can cancel it
        {
            state = JobState.CANCELLED;
        }

        return true;
    }

}
