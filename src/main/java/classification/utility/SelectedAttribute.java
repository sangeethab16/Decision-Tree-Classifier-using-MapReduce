package classification.utility;

public class SelectedAttribute {
    private int ak;
    private float cpk;

    public SelectedAttribute(int ak, float cpk) {
        this.ak = ak;
        this.cpk = cpk;
    }

    public int getAk() {
        return ak;
    }

    public void setAk(int ak) {
        this.ak = ak;
    }

    public float getCpk() {
        return cpk;
    }

    public void setCpk(float cpk) {
        this.cpk = cpk;
    }
}
