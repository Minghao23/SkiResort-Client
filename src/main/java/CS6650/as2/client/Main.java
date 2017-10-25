package CS6650.as2.client;

import CS6650.as2.model.MyVert;
import CS6650.as2.model.Record;
import CS6650.as2.util.Stat;
import org.glassfish.jersey.client.ClientProperties;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Main {

    static final private String protocol = "http";
    static final private String host = "54.200.82.186";
    //static final private String host = "localhost";
    static final private int port = 8080;

    static final String fileURL1 = "/Users/hu_minghao/CS6650/Assignment2/SkiResort-Client/files/BSDSAssignment2Day1.csv";
    static final String fileURL2 = "/Users/hu_minghao/CS6650/Assignment2/SkiResort-Client/files/BSDSAssignment2Day2.csv";
    ArrayList<Record> records = new ArrayList<Record>();


    public void outputData(ArrayList<Record> Records) {
        // System independent newline
        String newline = System.getProperty("line.separator");
        int count = 0;
        System.out.println("===Array List contents");
        for(Record tmp: Records){
            System.out.print(String.valueOf (tmp.getRecordID()) +  " " +
                    String.valueOf (tmp.getDayNum()) +  " " +
                    String.valueOf (tmp.getSkierID()) +  " " +
                    String.valueOf (tmp.getLiftID()) +  " " +
                    String.valueOf (tmp.getTime()) + newline
            );
            count++;
        }
        System.out.println("Rec Count = " + count);
    }

    public void readFileData(String fileURL) {

        if (!records.isEmpty()) {
            records.clear();
        }

        try {
            // cannot read .ser file because of the wrong serializedID and wrong package name
            // read .csv data
            System.out.println(">>>>>> Reading array list");

            BufferedReader br = new BufferedReader(new FileReader(fileURL));
            String line = br.readLine(); // skip the first line
            while ((line = br.readLine()) != null){
                String[] fields = line.split(",");
                Record record = new Record(
                        Integer.parseInt(fields[2]), // Column skier
                        Integer.parseInt(fields[3]), // Column lift
                        Integer.parseInt(fields[1]), // Column day
                        Integer.parseInt(fields[4])); // Column time
                records.add(record);
            }

            br.close();
            System.out.println(">>>>>> Reading complete.");

        }catch(IOException ioe){
            ioe.printStackTrace();
        }
    }

    public void postTasks(int taskSize) {
        // apply multi-thread
        System.out.println(">>>>>> Start POST requests...");
        String api = "/SkiResort/rest/hello/load";
        long startTime = System.currentTimeMillis();
        Client client = ClientBuilder.newClient();
        client.property(ClientProperties.CONNECT_TIMEOUT, 9999999);
        client.property(ClientProperties.READ_TIMEOUT,    9999999);
        ArrayList<PostRecord> postTasks = new ArrayList<PostRecord>();
        Stat stat = new Stat();
        for (int i = 0; i < records.size(); i++) { // test in 10000 data
            postTasks.add(new PostRecord(protocol, host, port, api, records.get(i), client, stat));
        }
        ExecutorService pool = Executors.newFixedThreadPool(taskSize);
        try {
            pool.invokeAll(postTasks);
            pool.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        client.close();
        System.out.println(">>>>>> Session ends");
        System.out.println();
        System.out.println(">>>>>> STATISTICS <<<<<<");
        System.out.println("> Number of threads: " + taskSize);
        System.out.println("> Total post time (wall time): " + (System.currentTimeMillis() - startTime));
        System.out.println("> Total request sent: " + stat.getSentRequestsNum());
        System.out.println("> Total successful request: " + stat.getSuccessRequestsNum());
        System.out.println("> Mean latency: " + stat.getMeanLatency());
        System.out.println("> Median latency: " + stat.getMedianLatency());
        System.out.println("> 95th percentile latency: " + stat.get95thLatency());
        System.out.println("> 99th percentile latency: " + stat.get99thLatency());

    }

    public void getTasks(int dayNum) {
        // apply multi-thread
        System.out.println(">>>>>> Start GET requests...");
        long startTime = System.currentTimeMillis();
        Client client = ClientBuilder.newClient();
        ArrayList<GetMyVert> getMyVerts = new ArrayList<GetMyVert>();
        Stat stat = new Stat();
        for (int i = 1; i <= 40000; i++) {
            getMyVerts.add(new GetMyVert(protocol, host, port, i, dayNum, client, stat));
        }
        ExecutorService pool = Executors.newFixedThreadPool(100);// fix in 100 threads
        try {
            List<Future<MyVert>> futures = pool.invokeAll(getMyVerts);
            pool.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        client.close();
        System.out.println(">>>>>> Session ends");
        System.out.println();
        System.out.println(">>>>>> STATISTICS <<<<<<");
        System.out.println("> Number of threads: 100");
        System.out.println("> Total run time: " + (System.currentTimeMillis() - startTime));
        System.out.println("> Total request sent: " + stat.getSentRequestsNum());
        System.out.println("> Total successful request: " + stat.getSuccessRequestsNum());
        System.out.println("> Mean latency: " + stat.getMeanLatency());
        System.out.println("> Median latency: " + stat.getMedianLatency());
        System.out.println("> 95th percentile latency: " + stat.get95thLatency());
        System.out.println("> 99th percentile latency: " + stat.get99thLatency());
    }

    public static void main(String[] args) {
        Main main = new Main();

        if(args[0].equals("upload")) {
            main.readFileData(Integer.parseInt(args[2]) == 1 ? fileURL1 : fileURL2);
            main.postTasks(Integer.parseInt(args[1]));
        }

        if(args[0].equals("get")) {
            main.getTasks(Integer.parseInt(args[1]));
        }
    }
}

