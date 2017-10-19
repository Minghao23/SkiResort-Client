package CS6650.as2.client;

import CS6650.as2.model.MyVert;
import CS6650.as2.model.RFIDLiftData;
import CS6650.as2.util.Stat;

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
    static final private String host = "localhost";
    static final private int port = 8080;

    static final String fileURL = "/Users/hu_minghao/CS6650/Assignment2/SkiResort-Client/files/BSDSAssignment2Day1.csv";
    ArrayList<RFIDLiftData> RFIDDataIn = new ArrayList<RFIDLiftData>();


    public void outputData(ArrayList<RFIDLiftData> RFIDDataIn) {
        // System independent newline
        String newline = System.getProperty("line.separator");
        int count = 0;
        System.out.println("===Array List contents");
        for(RFIDLiftData tmp: RFIDDataIn){
            System.out.print(String.valueOf (tmp.getResortID()) +  " " +
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

        if (!RFIDDataIn.isEmpty()) {
            RFIDDataIn.clear();
        }

        try {
            // cannot read .ser file because of the wrong serializedID and wrong package name
            // read .csv data
            System.out.println(">>>>>> Reading array list");

            BufferedReader br = new BufferedReader(new FileReader(fileURL));
            String line = br.readLine(); // skip the first line
            while ((line = br.readLine()) != null){
                String[] fields = line.split(",");
                RFIDLiftData rfidLiftData = new RFIDLiftData(
                        Integer.parseInt(fields[0]),
                        Integer.parseInt(fields[1]),
                        Integer.parseInt(fields[2]),
                        Integer.parseInt(fields[3]),
                        Integer.parseInt(fields[4]));
                RFIDDataIn.add(rfidLiftData);
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
        long startTime = System.currentTimeMillis();
        Client client = ClientBuilder.newClient();
        ArrayList<PostRFIDData> postTasks = new ArrayList<PostRFIDData>();
        Stat stat = new Stat();
        for (int i = 0; i < 10000; i++) { // test in 10000 data
            postTasks.add(new PostRFIDData(protocol, host, port, "/rest/hello/load", RFIDDataIn.get(i), client, stat));
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
        System.out.println("> Total runtime: " + (System.currentTimeMillis() - startTime));
        System.out.println("> Total latency: " + stat.getTotalLatency());
        System.out.println("> Total request sent: " + stat.getSentRequestsNum());
        System.out.println("> Total successful request: " + stat.getSuccessRequestsNum());
        System.out.println("> Mean latency: " + stat.getMeanLatency());
        System.out.println("> Median latency: " + stat.getMedianLatency());
        System.out.println("> 95th percentile latency: " + stat.get95thLatency());
        System.out.println("> 99th percentile latency: " + stat.get99thLatency());

    }

    public void singleGetTask(int skierID, int dayNum) {
        // maybe don't need multi-thread?
        Client client = ClientBuilder.newClient();
        String api = "/rest/hello/myvert/" + skierID + "&" + dayNum;
        Stat stat = new Stat();
        GetMyVert getMyVert = new GetMyVert(protocol, host, port, skierID, dayNum, client, stat);
        System.out.println(getMyVert.call().toString());
        client.close();
    }

    public void getTasks(int dayNum, int taskSize) {
        // apply multi-thread
        System.out.println(">>>>>> Start GET requests...");
        long startTime = System.currentTimeMillis();
        Client client = ClientBuilder.newClient();
        ArrayList<GetMyVert> getMyVerts = new ArrayList<GetMyVert>();
        Stat stat = new Stat();
        for (int i = 0; i < 10000; i++) { // test in 10000 data
            getMyVerts.add(new GetMyVert(protocol, host, port, RFIDDataIn.get(i).getSkierID(), dayNum, client, stat));
        }
        ExecutorService pool = Executors.newFixedThreadPool(taskSize);
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
        System.out.println("> Number of threads: " + taskSize);
        System.out.println("> Total runtime: " + (System.currentTimeMillis() - startTime));
        System.out.println("> Total latency: " + stat.getTotalLatency());
        System.out.println("> Total request sent: " + stat.getSentRequestsNum());
        System.out.println("> Total successful request: " + stat.getSuccessRequestsNum());
        System.out.println("> Mean latency: " + stat.getMeanLatency());
        System.out.println("> Median latency: " + stat.getMedianLatency());
        System.out.println("> 95th percentile latency: " + stat.get95thLatency());
        System.out.println("> 99th percentile latency: " + stat.get99thLatency());
    }

    public static void main(String[] args) {
        Main main = new Main();
        main.readFileData(fileURL);
        //main.postTasks(10);
        main.getTasks(1,10);
        //main.singleGetTask(17,1);
    }
}

