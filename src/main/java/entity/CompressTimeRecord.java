package entity;


public class CompressTimeRecord {
    public long encodingTime;
    public long decodingTime;
    public long totalTime;
    public long zipEncodingTime;
    public long unZipDecodingTime;
    public long zippedTotalTime;
    public double codingRatio;
    public double zippedCodingRatio;

    public long getEncodingTime() {
        return encodingTime;
    }

    public void setEncodingTime(long encodingTime) {
        this.encodingTime = encodingTime;
    }

    public long getDecodingTime() {
        return decodingTime;
    }

    public void setDecodingTime(long decodingTime) {
        this.decodingTime = decodingTime;
    }

    public long getTotalTime() {
        return totalTime;
    }

    public void setTotalTime(long totalTime) {
        this.totalTime = totalTime;
    }

    public long getZipEncodingTime() {
        return zipEncodingTime;
    }

    public void setZipEncodingTime(long zipEncodingTime) {
        this.zipEncodingTime = zipEncodingTime;
    }

    public long getUnZipDecodingTime() {
        return unZipDecodingTime;
    }

    public void setUnZipDecodingTime(long unZipDecodingTime) {
        this.unZipDecodingTime = unZipDecodingTime;
    }

    public long getZippedTotalTime() {
        return zippedTotalTime;
    }

    public void setZippedTotalTime(long zippedTotalTime) {
        this.zippedTotalTime = zippedTotalTime;
    }

    public double getCodingRatio() {
        return codingRatio;
    }

    public void setCodingRatio(double codingRatio) {
        this.codingRatio = codingRatio;
    }

    public double getZippedCodingRatio() {
        return zippedCodingRatio;
    }

    public void setZippedCodingRatio(double zippedCodingRatio) {
        this.zippedCodingRatio = zippedCodingRatio;
    }
}
