package CS6650.as2.client;

import CS6650.as2.model.RFIDLiftData;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.util.ArrayList;

/**
 *
 * @author Ian Gorton
 */
public class BSDSAss2TestData {


    public static void main(String[] args) {
        
        // System independe nt newline
        String newline = System.getProperty("line.separator");
        // file and stream for input
        FileInputStream fis = null;
        ObjectInputStream ois = null;
        
        try
        {
            // TO DO change file path for your system
            fis = new FileInputStream("BSDSAssignment2Day1.ser");
            ois = new ObjectInputStream(fis);
            ArrayList <RFIDLiftData> RFIDDataIn = new ArrayList<RFIDLiftData>();
            
            // read data from serialized file
            System.out.println("===Reading array list");
            RFIDDataIn = (ArrayList) ois.readObject();
            
            // output contents to console
            int count = 0;
            System.out.println("===Array List contents");
            for(RFIDLiftData tmp: RFIDDataIn){
                System.out.print(String.valueOf (tmp.getResortID()) +  " " +
                                    String.valueOf (tmp.getDayNum()) +  " " +
                                    String.valueOf (tmp.getSkierID()) +  " " +
                                    String.valueOf (tmp.getLiftID()) +  " " +    
                                    String.valueOf (tmp.getTime()) +   newline                
                );
                count++;
            }
            System.out.println("Rec Count = " + count);
            
            ois.close();
            fis.close();
            
         }catch(IOException ioe){
             ioe.printStackTrace();
             return;
         }catch(ClassNotFoundException c){
             System.out.println("Class not found");
             c.printStackTrace();
             return;
          } 
    }
}
        
               
 
