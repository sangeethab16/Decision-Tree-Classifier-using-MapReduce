package classification.utility;

public class RatioCutPoint {
    private float ratio;
    private double cutPoint;

    public RatioCutPoint(float ratio, double cutPoint) {
        this.ratio = ratio;
        this.cutPoint = cutPoint;
    }

    public float getRatio() {
        return ratio;
    }

    public void setRatio(float ratio) {
        this.ratio = ratio;
    }

    public double getCutPoint() {
        return cutPoint;
    }

    public void setCutPoint(double cutPoint) {
        this.cutPoint = cutPoint;
    }
}
