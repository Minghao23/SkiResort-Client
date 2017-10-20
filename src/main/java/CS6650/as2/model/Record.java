package CS6650.as2.model;

/**
 * Created by hu_minghao on 10/17/17.
 */
public class Record {

    private int recordID;
    private int skierID;
    private int liftID;
    private int dayNum;
    private int time;

    public Record() {
    }

    public Record(int recordID) {
        this.recordID = recordID;
    }

    public Record(int skierID, int liftID, int dayNum, int time) {
        this.skierID = skierID;
        this.liftID = liftID;
        this.dayNum = dayNum;
        this.time = time;
    }

    public Record(int recordID, int skierID, int liftID, int dayNum, int time) {
        this.recordID = recordID;
        this.skierID = skierID;
        this.liftID = liftID;
        this.dayNum = dayNum;
        this.time = time;
    }

    public int getRecordID() {
        return recordID;
    }

    public void setRecordID(int recordID) {
        this.recordID = recordID;
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

    public int getDayNum() {
        return dayNum;
    }

    public void setDayNum(int dayNum) {

        this.dayNum = dayNum;
    }

    public int getTime() {
        return time;
    }

    public void setTime(int time) {
        this.time = time;
    }

    @Override
    public String toString() {
        return "RecordID: " + this.recordID + "  SkierID: " + this.skierID + "  Lift: " +
                this.liftID + "  Day: " + this.dayNum + "  Time: " + this.time;
    }
}
