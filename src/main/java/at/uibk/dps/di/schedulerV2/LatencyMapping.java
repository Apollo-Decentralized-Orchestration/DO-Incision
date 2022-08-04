package at.uibk.dps.di.schedulerV2;

public class LatencyMapping {
    String node1;
    String node2;
    double latency;

    public LatencyMapping(String node1, String node2, double latency) {
        this.node1 = node1;
        this.node2 = node2;
        this.latency = latency;
    }

    public String getNode1() {
        return node1;
    }

    public void setNode1(String node1) {
        this.node1 = node1;
    }

    public String getNode2() {
        return node2;
    }

    public void setNode2(String node2) {
        this.node2 = node2;
    }

    public double getLatency() {
        return latency;
    }

    public void setLatency(double latency) {
        this.latency = latency;
    }
}
