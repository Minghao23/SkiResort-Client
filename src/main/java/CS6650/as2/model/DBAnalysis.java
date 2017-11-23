package CS6650.as2.model;

/**
 * Created by hu_minghao on 11/22/17.
 */
public class DBAnalysis {
    private int totalRequestNum;
    private long meanLatency;
    private long medianLatency;
    private long the95thLatency;
    private long the99thLatency;

    public DBAnalysis() {
    }

    public DBAnalysis(int totalRequestNum, long meanLatency, long medianLatency, long the95thLatency, long the99thLatency) {
        this.totalRequestNum = totalRequestNum;
        this.meanLatency = meanLatency;
        this.medianLatency = medianLatency;
        this.the95thLatency = the95thLatency;
        this.the99thLatency = the99thLatency;
    }

    public int getTotalRequestNum() {
        return totalRequestNum;
    }

    public void setTotalRequestNum(int totalRequestNum) {
        this.totalRequestNum = totalRequestNum;
    }

    public long getMeanLatency() {
        return meanLatency;
    }

    public void setMeanLatency(long meanLatency) {
        this.meanLatency = meanLatency;
    }

    public long getMedianLatency() {
        return medianLatency;
    }

    public void setMedianLatency(long medianLatency) {
        this.medianLatency = medianLatency;
    }

    public long getThe95thLatency() {
        return the95thLatency;
    }

    public void setThe95thLatency(long the95thLatency) {
        this.the95thLatency = the95thLatency;
    }

    public long getThe99thLatency() {
        return the99thLatency;
    }

    public void setThe99thLatency(long the99thLatency) {
        this.the99thLatency = the99thLatency;
    }
}
