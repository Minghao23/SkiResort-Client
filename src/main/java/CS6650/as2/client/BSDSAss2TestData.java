package CS6650.as2.client;

import CS6650.as2.model.RFIDLiftData;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Entity;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.io.*;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.ArrayList;


public class BSDSAss2TestData {

    private String protocol = "http";
    private String api = "/rest/hello/load";
    private String host = "localhost";
    private int port = 8080;

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
            //if(count == 10) break;
        }
        System.out.println("Rec Count = " + count);
    }

    public void doPost(Client client, RFIDLiftData data) throws MalformedURLException {
        URL url = new URL(protocol, host, port, api);
        WebTarget webTarget = client.target(url.toString());
        Response response = null;
//        boolean isSent = false;
        try {
            response = webTarget.request().post(Entity.json(data));
            System.out.println(response.readEntity(String.class));
            response.close();
//            isSent = true;
        } catch (ProcessingException e) {
            e.printStackTrace();
        }
    }


    public static void main(String[] args) {

        // file and stream for input
        FileInputStream fis = null;
        ObjectInputStream ois = null;
        String fileURL = "/Users/hu_minghao/CS6650/Assignment2/SkiResort-Client/files/BSDSAssignment2Day1.csv";
        ArrayList <RFIDLiftData> RFIDDataIn = new ArrayList<RFIDLiftData>();
        try {
            // cannot read .ser file because of the wrong serializedID and wrong package name
            // read .csv data
            System.out.println("===Reading array list");

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
            
        }catch(IOException ioe){
            ioe.printStackTrace();
            return;
        }

         // post all data
        BSDSAss2TestData bsdsAss2TestData = new BSDSAss2TestData();
        Client client = ClientBuilder.newClient();
        long startTime = System.currentTimeMillis();
//        for (int i = 0; i < RFIDDataIn.size(); ++i) {
        for (int i = 0; i < 1000; ++i) {
            try {
                bsdsAss2TestData.doPost(client, RFIDDataIn.get(i));
            } catch (Exception e){
                System.out.println("has problems on post");
            }
        }
        long totalTime = System.currentTimeMillis() - startTime;
        System.out.println("Total runtime: " + totalTime);
        client.close();

    }
}
        
               
 
