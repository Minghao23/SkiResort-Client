package CS6650.as2.client;

import CS6650.as2.model.MyVert;
import CS6650.as2.util.Stat;

import javax.ws.rs.ProcessingException;
import javax.ws.rs.client.Client;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import java.net.MalformedURLException;
import java.net.URL;
import java.util.concurrent.Callable;

/**
 * Created by hu_minghao on 10/16/17.
 */
public class GetMyVert implements Callable<MyVert> {

    private String protocol;
    private String host;
    private int port;
    private int skierID;
    private int dayNum;
    Client client;
    Stat stat;

    public GetMyVert(String protocol, String host, int port, int skierID, int dayNum, Client client, Stat stat) {
        this.protocol = protocol;
        this.host = host;
        this.port = port;
        this.skierID = skierID;
        this.dayNum = dayNum;
        this.client = client;
        this.stat = stat;
    }

    public MyVert call() {
        URL url = null;
        String api = "/SkiResort-Server_war/rest/hello/myvert/" + skierID + "&" + dayNum;
        try{
            url = new URL(protocol, host, port, api);
        } catch (MalformedURLException e){
            e.printStackTrace();
        }
        WebTarget webTarget = client.target(url.toString());
        Response response;
        MyVert result = null;
        long startTime = System.currentTimeMillis();
        try {
            response = webTarget.request().get();
            result = response.readEntity(MyVert.class);
            System.out.println(result.toString());
            response.close();
            stat.recordSentRequestNum();
            stat.recordSuccessfulRequestNum(response.getStatus() == 200);
        } catch (ProcessingException e) {
            e.printStackTrace();
        }
        long latency = System.currentTimeMillis() - startTime;
        stat.recordLatency(latency);
        return result;
    }
}
