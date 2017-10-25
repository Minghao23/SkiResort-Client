package CS6650.as2.model;

/**
 * Created by hu_minghao on 10/17/17.
 */
public class MyVert {

    private int skierID;
    private int dayNum;
    private int vertical;
    private int liftTimes;

    public MyVert() {
    }

    public MyVert(int skierID, int dayNum, int vertical, int liftTimes) {
        this.skierID = skierID;
        this.dayNum = dayNum;
        this.vertical = vertical;
        this.liftTimes = liftTimes;
    }

    public int getSkierID() {
        return skierID;
    }

    public void setSkierID(int skierID) {
        this.skierID = skierID;
    }

    public int getDayNum() {
        return dayNum;
    }

    public void setDayNum(int dayNum) {
        this.dayNum = dayNum;
    }

    public int getVertical() {
        return vertical;
    }

    public void setVertical(int vertical) {
        this.vertical = vertical;
    }

    public int getLiftTimes() {
        return liftTimes;
    }

    public void setLiftTimes(int liftTimes) {
        this.liftTimes = liftTimes;
    }

    public String toString() {
        return "SkierID: " + skierID + "  DayNum: " + dayNum + "  Total vertical: " + vertical + "  Number of lift ride: " + liftTimes;
    }
}
