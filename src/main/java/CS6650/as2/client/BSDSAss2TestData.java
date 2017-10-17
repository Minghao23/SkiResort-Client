package CS6650.as2.client;

import CS6650.as2.model.RFIDLiftData;
import CS6650.as2.util.Stat;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.ArrayList;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;


public class BSDSAss2TestData {

    static private String protocol = "http";
    static private String host = "localhost";
    static private int port = 8080;
    static private String api = "/rest/hello/load";

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

    public static void main(String[] args) {

        // file and stream for input
        String fileURL = "/Users/hu_minghao/CS6650/Assignment2/SkiResort-Client/files/BSDSAssignment2Day1.csv";
        ArrayList <RFIDLiftData> RFIDDataIn = new ArrayList<RFIDLiftData>();
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
            System.out.println(">>>>>> Reading complete");

        }catch(IOException ioe){
            ioe.printStackTrace();
            return;
        }

         // post all data
        System.out.println(">>>>>> Start posting request");
        int taskSize = 10;
        Client client = ClientBuilder.newClient();
        ArrayList<PostRFIDData> postTasks = new ArrayList<PostRFIDData>();
        //for (int i = 0; i < RFIDDataIn.size(); i++) {
        Stat stat = new Stat();
        for (int i = 0; i < 10000; i++) {
            postTasks.add(new PostRFIDData(protocol, host, port, api, RFIDDataIn.get(i), client, stat));
        }
        ExecutorService pool = Executors.newFixedThreadPool(taskSize);
        try {
            pool.invokeAll(postTasks);
            pool.shutdown();
        } catch (InterruptedException e) {
            e.printStackTrace();
        }

        client.close();
        System.out.println(">>>>>> Total latency: " + stat.getTotalLatency());
        System.out.println(">>>>>> Total request sent: " + stat.getSentRequestsNum());
        System.out.println(">>>>>> Total successful request: " + stat.getSuccessRequestsNum());

    }
}
        
               
 
