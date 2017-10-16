/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package CS6650.as2.model;

import java.io.Serializable;

/**
 *
 * Simple class to wrap the data in a RFID lift pass reader record
 */
public class RFIDLiftData implements Serializable, Comparable<RFIDLiftData>  {
    
    private int resortID;
    private int dayNum;
    private int skierID;
    private int liftID;
    private int time;

    public RFIDLiftData() {

    }

    public RFIDLiftData(int resortID, int dayNum, int skierID, int liftID, int time) {
        this.resortID = resortID;
        this.dayNum = dayNum;
        this.skierID = skierID;
        this.liftID = liftID;
        this.time = time;
    }

    public int getResortID() {
        return resortID;
    }

    public void setResortID(int resortID) {
        this.resortID = resortID;
    }

    public int getDayNum() {
        return dayNum;
    }

    public void setDayNum(int dayNum) {
        this.dayNum = dayNum;
    }

    public int getSkierID() {
        return skierID;
    }

    public void setSkierID(int skierID) {
        this.skierID = skierID;
    }

    public int getLiftID() {
        return liftID;
    }

    public void setLiftID(int liftID) {
        this.liftID = liftID;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }
   
    public int compareTo(RFIDLiftData compareData) {
        int compareTime = ((RFIDLiftData) compareData).getTime();
        
        //ascending order
        return this.time - compareTime ;
    }

    @Override
    public String toString() {
        return this.resortID + " " + this.dayNum + " " + this.skierID + " " +
                this.liftID + " " + this.time + " ";
    }
}
