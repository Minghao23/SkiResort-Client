package CS6650.as2.util;

/**
 * Created by hu_minghao on 10/16/17.
 */
public class Stat {
    private int successRequestsNum = 0;
    private int sentRequestsNum = 0;
    private long totalLatency = 0;

    public void recordLatency(long latency) {
        totalLatency += latency;
    }

    public void recordSentRequestNum() {
        sentRequestsNum += 1;
    }

    public void recordSuccessfulRequestNum(boolean isSuccess) {
        successRequestsNum += isSuccess ? 1 : 0;
    }

    public int getSuccessRequestsNum() {
        return successRequestsNum;
    }

    public int getSentRequestsNum() {
        return sentRequestsNum;
    }

    public long getTotalLatency() {
        return totalLatency;
    }
}
