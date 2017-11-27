package CS6650.as2.client;

import CS6650.as2.model.DBAnalysis;
import CS6650.as2.model.HttpAnalysis;
import CS6650.as2.model.MyVert;
import CS6650.as2.model.Record;
import CS6650.as2.util.Stat;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class Main {

    static final private String protocol = "http";
    static final private String host1 = "34.213.183.79";
    static final private String host2 = "54.149.109.52";
    static final private String host3 = "54.202.247.75";
    static final private String singlehost = "54.245.182.24";
    static final private String host = "myELB-979566651.us-west-2.elb.amazonaws.com";
//    static final private String host = "localhost";
    static final private int port = 8080;

    static final String DAY1URL = "/Users/hu_minghao/CS6650/Assignment2/SkiResort-Client/files/BSDSAssignment2Day1.csv";
    static final String DAY2URL = "/Users/hu_minghao/CS6650/Assignment2/SkiResort-Client/files/BSDSAssignment2Day2.csv";
    static final String DAY3URL = "/Users/hu_minghao/CS6650/Assignment2/SkiResort-Client/files/BSDSAssignment2Day3.csv";
    static final String DAY4URL = "/Users/hu_minghao/CS6650/Assignment2/SkiResort-Client/files/BSDSAssignment2Day4.csv";
    static final String DAY5URL = "/Users/hu_minghao/CS6650/Assignment2/SkiResort-Client/files/BSDSAssignment2Day5.csv";
    static final String DAY999URL = "/Users/hu_minghao/CS6650/Assignment2/SkiResort-Client/files/BSDSAssignment2Day999.csv";
    ArrayList<Record> records = new ArrayList<Record>();


    public void readFileData(String fileURL) {
        if(fileURL == null) {
            System.out.println(">>>>>> No input file");
            return;
        }

        if (!records.isEmpty()) {
            records.clear();
        }

        try {
            // cannot read .ser file because of the wrong serializedID and wrong package name
            // read .csv data
            System.out.println(">>>>>> Reading csv data...");

            BufferedReader br = new BufferedReader(new FileReader(fileURL));
            String line;
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
            System.out.println(">>>>>> Reading complete");

        }catch(IOException ioe){
            ioe.printStackTrace();
        }
    }

    public void postTasks(int taskSize) {
        // apply multi-thread
        System.out.println(">>>>>> Start POST requests...");
        String api = "/SkiResort-Server_war/rest/hello/load";
        long startTime = System.currentTimeMillis();
        Client client = ClientBuilder.newClient();
        ArrayList<PostRecord> postTasks = new ArrayList<PostRecord>();
        Stat stat = new Stat();
        for (int i = 0; i < records.size(); i++) {
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
        long walltime = System.currentTimeMillis() - startTime;

        System.out.println(">>>>>> Session ends");
        System.out.println();
        System.out.println(">>>>>> STATISTICS <<<<<<");
        System.out.println("> Number of threads: " + taskSize);
        System.out.println("> Total post time (wall time): " + walltime + "ms");
        System.out.println("> Total request sent: " + stat.getSentRequestsNum());
        System.out.println("> Total successful request: " + stat.getSuccessRequestsNum());
        System.out.println("> Requests per second: " + stat.getReqPerSec(walltime));
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
        for (int i = 1; i <= 10000; i++) {
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
        long walltime = System.currentTimeMillis() - startTime;

        System.out.println(">>>>>> Session ends");
        System.out.println();
        System.out.println(">>>>>> STATISTICS <<<<<<");
        System.out.println("> Number of threads: 100");
        System.out.println("> Total get time (wall time): " + walltime + "ms");
        System.out.println("> Total request sent: " + stat.getSentRequestsNum());
        System.out.println("> Total successful request: " + stat.getSuccessRequestsNum());
        System.out.println("> Requests per second: " + stat.getReqPerSec(walltime));
        System.out.println("> Mean latency: " + stat.getMeanLatency());
        System.out.println("> Median latency: " + stat.getMedianLatency());
        System.out.println("> 95th percentile latency: " + stat.get95thLatency());
        System.out.println("> 99th percentile latency: " + stat.get99thLatency());
    }

    public void getHttpAnalysis() {
        Client client = ClientBuilder.newClient();
        URL url = null;
        String api = "/SkiResort-Server_war/rest/analysis/http";
        try{
            url = new URL(protocol, host, port, api);
        } catch (MalformedURLException e){
            e.printStackTrace();
        }
        WebTarget webTarget = client.target(url.toString());
        Response response;
        HttpAnalysis result;
        try {
            response = webTarget.request().get();
            result = response.readEntity(HttpAnalysis.class);
            response.close();
            client.close();
            System.out.println(">>>>>> Http Requests Analysis <<<<<<");
            System.out.println("");
            System.out.println("---- All three servers ----");
            System.out.println("> Number of successful requests: " + result.getAll_RequestNum());
            System.out.println("> Number of failed requests: " + result.getAll_failedRequestNum());
            System.out.println("> Mean latency: " + result.getAll_meanLatency());
            System.out.println("> Median latency: " + result.getAll_medianLatency());
            System.out.println("> 95th latency: " + result.getAll_the95thLatency());
            System.out.println("> 99th latency: " + result.getAll_the99thLatency());
            System.out.println("");
            System.out.println("---- Server 1 ----");
            System.out.println("> Number of successful requests: " + result.getH1_RequestNum());
            System.out.println("> Number of failed requests: " + result.getH1_failedRequestNum());
            System.out.println("> Mean latency: " + result.getH1_meanLatency());
            System.out.println("> Median latency: " + result.getH1_medianLatency());
            System.out.println("> 95th latency: " + result.getH1_the95thLatency());
            System.out.println("> 99th latency: " + result.getH1_the99thLatency());
            System.out.println("");
            System.out.println("---- Server 2 ----");
            System.out.println("> Number of successful requests: " + result.getH2_RequestNum());
            System.out.println("> Number of failed requests: " + result.getH2_failedRequestNum());
            System.out.println("> Mean latency: " + result.getH2_meanLatency());
            System.out.println("> Median latency: " + result.getH2_medianLatency());
            System.out.println("> 95th latency: " + result.getH2_the95thLatency());
            System.out.println("> 99th latency: " + result.getH2_the99thLatency());
            System.out.println("");
            System.out.println("---- Server 3 ----");
            System.out.println("> Number of successful requests: " + result.getH3_RequestNum());
            System.out.println("> Number of failed requests: " + result.getH3_failedRequestNum());
            System.out.println("> Mean latency: " + result.getH3_meanLatency());
            System.out.println("> Median latency: " + result.getH3_medianLatency());
            System.out.println("> 95th latency: " + result.getH3_the95thLatency());
            System.out.println("> 99th latency: " + result.getH3_the99thLatency());
        } catch (ProcessingException e) {
            e.printStackTrace();
        }
    }

    public void getDBAnalysis() {
        Client client = ClientBuilder.newClient();
        URL url = null;
        String api = "/SkiResort-Server_war/rest/analysis/db";
        try{
            url = new URL(protocol, host, port, api);
        } catch (MalformedURLException e){
            e.printStackTrace();
        }
        WebTarget webTarget = client.target(url.toString());
        Response response;
        DBAnalysis result;
        try {
            response = webTarget.request().get();
            result = response.readEntity(DBAnalysis.class);
            response.close();
            client.close();
            System.out.println(">>>>>> Database Queries Analysis <<<<<<");
            System.out.println("");
            System.out.println("---- All three servers ----");
            System.out.println("> Number of queries: " + result.getAll_QueryNum());
            System.out.println("> Mean latency: " + result.getAll_meanLatency());
            System.out.println("> Median latency: " + result.getAll_medianLatency());
            System.out.println("> 95th latency: " + result.getAll_the95thLatency());
            System.out.println("> 99th latency: " + result.getAll_the99thLatency());
            System.out.println("");
            System.out.println("---- Server 1 ----");
            System.out.println("> Number of queries:: " + result.getH1_QueryNum());
            System.out.println("> Mean latency: " + result.getH1_meanLatency());
            System.out.println("> Median latency: " + result.getH1_medianLatency());
            System.out.println("> 95th latency: " + result.getH1_the95thLatency());
            System.out.println("> 99th latency: " + result.getH1_the99thLatency());
            System.out.println("");
            System.out.println("---- Server 2 ----");
            System.out.println("> Number of queries: " + result.getH2_QueryNum());
            System.out.println("> Mean latency: " + result.getH2_meanLatency());
            System.out.println("> Median latency: " + result.getH2_medianLatency());
            System.out.println("> 95th latency: " + result.getH2_the95thLatency());
            System.out.println("> 99th latency: " + result.getH2_the99thLatency());
            System.out.println("");
            System.out.println("---- Server 3 ----");
            System.out.println("> Number of queries: " + result.getH3_QueryNum());
            System.out.println("> Mean latency: " + result.getH3_meanLatency());
            System.out.println("> Median latency: " + result.getH3_medianLatency());
            System.out.println("> 95th latency: " + result.getH3_the95thLatency());
            System.out.println("> 99th latency: " + result.getH3_the99thLatency());
        } catch (ProcessingException e) {
            e.printStackTrace();
        }
    }

    public static void main(String[] args) {
        Main main = new Main();

        if(args[0].equals("upload")) {
            String file = null;
            switch (Integer.parseInt(args[2])) {
                case 1 : file = DAY1URL; break;
                case 2 : file = DAY2URL;break;
                case 3 : file = DAY3URL;break;
                case 4 : file = DAY4URL;break;
                case 5 : file = DAY5URL;break;
                case 999 : file = DAY999URL;break;
            }
            main.readFileData(file);
            main.postTasks(Integer.parseInt(args[1]));
        }

        if(args[0].equals("get")) {
            main.getTasks(Integer.parseInt(args[1]));
        }

        if(args[0].equals("getHttpAnalysis")) {
            main.getHttpAnalysis();
        }

        if(args[0].equals("getDBAnalysis")) {
            main.getDBAnalysis();
        }
    }
}

