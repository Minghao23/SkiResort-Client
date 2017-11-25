package CS6650.as2.model;

/**
 * Created by hu_minghao on 11/22/17.
 */
public class HttpAnalysis {
    private long H1_RequestNum;
    private long H2_RequestNum;
    private long H3_RequestNum;
    private long All_RequestNum;
    private long H1_failedRequestNum;
    private long H2_failedRequestNum;
    private long H3_failedRequestNum;
    private long All_failedRequestNum;
    private long H1_meanLatency;
    private long H2_meanLatency;
    private long H3_meanLatency;
    private long All_meanLatency;
    private long H1_medianLatency;
    private long H2_medianLatency;
    private long H3_medianLatency;
    private long All_medianLatency;
    private long H1_the95thLatency;
    private long H2_the95thLatency;
    private long H3_the95thLatency;
    private long All_the95thLatency;
    private long H1_the99thLatency;
    private long H2_the99thLatency;
    private long H3_the99thLatency;
    private long All_the99thLatency;

    public HttpAnalysis() {
    }

    public HttpAnalysis(long h1_RequestNum, long h2_RequestNum, long h3_RequestNum, long all_RequestNum, long h1_failedRequestNum, long h2_failedRequestNum, long h3_failedRequestNum, long all_failedRequestNum, long h1_meanLatency, long h2_meanLatency, long h3_meanLatency, long all_meanLatency, long h1_medianLatency, long h2_medianLatency, long h3_medianLatency, long all_medianLatency, long h1_the95thLatency, long h2_the95thLatency, long h3_the95thLatency, long all_the95thLatency, long h1_the99thLatency, long h2_the99thLatency, long h3_the99thLatency, long all_the99thLatency) {
        H1_RequestNum = h1_RequestNum;
        H2_RequestNum = h2_RequestNum;
        H3_RequestNum = h3_RequestNum;
        All_RequestNum = all_RequestNum;
        H1_failedRequestNum = h1_failedRequestNum;
        H2_failedRequestNum = h2_failedRequestNum;
        H3_failedRequestNum = h3_failedRequestNum;
        All_failedRequestNum = all_failedRequestNum;
        H1_meanLatency = h1_meanLatency;
        H2_meanLatency = h2_meanLatency;
        H3_meanLatency = h3_meanLatency;
        All_meanLatency = all_meanLatency;
        H1_medianLatency = h1_medianLatency;
        H2_medianLatency = h2_medianLatency;
        H3_medianLatency = h3_medianLatency;
        All_medianLatency = all_medianLatency;
        H1_the95thLatency = h1_the95thLatency;
        H2_the95thLatency = h2_the95thLatency;
        H3_the95thLatency = h3_the95thLatency;
        All_the95thLatency = all_the95thLatency;
        H1_the99thLatency = h1_the99thLatency;
        H2_the99thLatency = h2_the99thLatency;
        H3_the99thLatency = h3_the99thLatency;
        All_the99thLatency = all_the99thLatency;
    }

    public long getH1_RequestNum() {
        return H1_RequestNum;
    }

    public long getH2_RequestNum() {
        return H2_RequestNum;
    }

    public long getH3_RequestNum() {
        return H3_RequestNum;
    }

    public long getAll_RequestNum() {
        return All_RequestNum;
    }

    public long getH1_failedRequestNum() {
        return H1_failedRequestNum;
    }

    public long getH2_failedRequestNum() {
        return H2_failedRequestNum;
    }

    public long getH3_failedRequestNum() {
        return H3_failedRequestNum;
    }

    public long getAll_failedRequestNum() {
        return All_failedRequestNum;
    }

    public long getH1_meanLatency() {
        return H1_meanLatency;
    }

    public long getH2_meanLatency() {
        return H2_meanLatency;
    }

    public long getH3_meanLatency() {
        return H3_meanLatency;
    }

    public long getAll_meanLatency() {
        return All_meanLatency;
    }

    public long getH1_medianLatency() {
        return H1_medianLatency;
    }

    public long getH2_medianLatency() {
        return H2_medianLatency;
    }

    public long getH3_medianLatency() {
        return H3_medianLatency;
    }

    public long getAll_medianLatency() {
        return All_medianLatency;
    }

    public long getH1_the95thLatency() {
        return H1_the95thLatency;
    }

    public long getH2_the95thLatency() {
        return H2_the95thLatency;
    }

    public long getH3_the95thLatency() {
        return H3_the95thLatency;
    }

    public long getAll_the95thLatency() {
        return All_the95thLatency;
    }

    public long getH1_the99thLatency() {
        return H1_the99thLatency;
    }

    public long getH2_the99thLatency() {
        return H2_the99thLatency;
    }

    public long getH3_the99thLatency() {
        return H3_the99thLatency;
    }

    public long getAll_the99thLatency() {
        return All_the99thLatency;
    }

    public void setH1_RequestNum(long h1_RequestNum) {
        H1_RequestNum = h1_RequestNum;
    }

    public void setH2_RequestNum(long h2_RequestNum) {
        H2_RequestNum = h2_RequestNum;
    }

    public void setH3_RequestNum(long h3_RequestNum) {
        H3_RequestNum = h3_RequestNum;
    }

    public void setAll_RequestNum(long all_RequestNum) {
        All_RequestNum = all_RequestNum;
    }

    public void setH1_failedRequestNum(long h1_failedRequestNum) {
        H1_failedRequestNum = h1_failedRequestNum;
    }

    public void setH2_failedRequestNum(long h2_failedRequestNum) {
        H2_failedRequestNum = h2_failedRequestNum;
    }

    public void setH3_failedRequestNum(long h3_failedRequestNum) {
        H3_failedRequestNum = h3_failedRequestNum;
    }

    public void setAll_failedRequestNum(long all_failedRequestNum) {
        All_failedRequestNum = all_failedRequestNum;
    }

    public void setH1_meanLatency(long h1_meanLatency) {
        H1_meanLatency = h1_meanLatency;
    }

    public void setH2_meanLatency(long h2_meanLatency) {
        H2_meanLatency = h2_meanLatency;
    }

    public void setH3_meanLatency(long h3_meanLatency) {
        H3_meanLatency = h3_meanLatency;
    }

    public void setAll_meanLatency(long all_meanLatency) {
        All_meanLatency = all_meanLatency;
    }

    public void setH1_medianLatency(long h1_medianLatency) {
        H1_medianLatency = h1_medianLatency;
    }

    public void setH2_medianLatency(long h2_medianLatency) {
        H2_medianLatency = h2_medianLatency;
    }

    public void setH3_medianLatency(long h3_medianLatency) {
        H3_medianLatency = h3_medianLatency;
    }

    public void setAll_medianLatency(long all_medianLatency) {
        All_medianLatency = all_medianLatency;
    }

    public void setH1_the95thLatency(long h1_the95thLatency) {
        H1_the95thLatency = h1_the95thLatency;
    }

    public void setH2_the95thLatency(long h2_the95thLatency) {
        H2_the95thLatency = h2_the95thLatency;
    }

    public void setH3_the95thLatency(long h3_the95thLatency) {
        H3_the95thLatency = h3_the95thLatency;
    }

    public void setAll_the95thLatency(long all_the95thLatency) {
        All_the95thLatency = all_the95thLatency;
    }

    public void setH1_the99thLatency(long h1_the99thLatency) {
        H1_the99thLatency = h1_the99thLatency;
    }

    public void setH2_the99thLatency(long h2_the99thLatency) {
        H2_the99thLatency = h2_the99thLatency;
    }

    public void setH3_the99thLatency(long h3_the99thLatency) {
        H3_the99thLatency = h3_the99thLatency;
    }

    public void setAll_the99thLatency(long all_the99thLatency) {
        All_the99thLatency = all_the99thLatency;
    }
}


