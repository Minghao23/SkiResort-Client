package CS6650.as2.model;

/**
 * Created by hu_minghao on 10/17/17.
 */
public class MyVert {
    int totalVertical;
    int liftTimes;

    public MyVert() {

    }
    public MyVert(int totalVertical, int liftTimes) {
        this.totalVertical = totalVertical;
        this.liftTimes = liftTimes;
    }

    public int getTotalVertical() {
        return totalVertical;
    }

    public void setTotalVertical(int totalVertical) {
        this.totalVertical = totalVertical;
    }

    public int getLiftTimes() {
        return liftTimes;
    }

    public void setLiftTimes(int liftTimes) {
        this.liftTimes = liftTimes;
    }

    public String toString() {
        return "Total vertical: " + totalVertical + "  Number of lift ride: " + liftTimes;
    }
}
