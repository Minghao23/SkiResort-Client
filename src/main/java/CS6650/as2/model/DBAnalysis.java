package CS6650.as2.model;

/**
 * Created by hu_minghao on 11/22/17.
 */
public class DBAnalysis {
    private long H1_QueryNum;
    private long H2_QueryNum;
    private long H3_QueryNum;
    private long All_QueryNum;
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

    public DBAnalysis() {
    }

    public DBAnalysis(long h1_QueryNum, long h2_QueryNum, long h3_QueryNum, long all_QueryNum, long h1_meanLatency, long h2_meanLatency, long h3_meanLatency, long all_meanLatency, long h1_medianLatency, long h2_medianLatency, long h3_medianLatency, long all_medianLatency, long h1_the95thLatency, long h2_the95thLatency, long h3_the95thLatency, long all_the95thLatency, long h1_the99thLatency, long h2_the99thLatency, long h3_the99thLatency, long all_the99thLatency) {
        H1_QueryNum = h1_QueryNum;
        H2_QueryNum = h2_QueryNum;
        H3_QueryNum = h3_QueryNum;
        All_QueryNum = all_QueryNum;
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

    public long getH1_QueryNum() {
        return H1_QueryNum;
    }

    public long getH2_QueryNum() {
        return H2_QueryNum;
    }

    public long getH3_QueryNum() {
        return H3_QueryNum;
    }

    public long getAll_QueryNum() {
        return All_QueryNum;
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

    public void setH1_QueryNum(long h1_QueryNum) {
        H1_QueryNum = h1_QueryNum;
    }

    public void setH2_QueryNum(long h2_QueryNum) {
        H2_QueryNum = h2_QueryNum;
    }

    public void setH3_QueryNum(long h3_QueryNum) {
        H3_QueryNum = h3_QueryNum;
    }

    public void setAll_QueryNum(long all_QueryNum) {
        All_QueryNum = all_QueryNum;
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


