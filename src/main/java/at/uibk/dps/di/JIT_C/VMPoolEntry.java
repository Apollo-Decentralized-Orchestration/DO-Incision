package at.uibk.dps.di.JIT_C;


import net.sf.opendse.model.Resource;

public class VMPoolEntry {

    private Resource type;
    private String id;
    private Double startTime;
    private Double expecteddIdleStartTime;
    private Double endTime;

    public VMPoolEntry(String id, Resource type, Double startTime, Double expecteddIdleStartTime,
        Double endTime) {
        this.id = id;
        this.type = type;
        this.startTime = startTime;
        this.expecteddIdleStartTime = expecteddIdleStartTime;
        this.endTime = endTime;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Resource getType() {
        return type;
    }

    public void setType(Resource type) {
        this.type = type;
    }

    public Double getStartTime() {
        return startTime;
    }

    public void setStartTime(Double startTime) {
        this.startTime = startTime;
    }

    public Double getExpecteddIdleStartTime() {
        return expecteddIdleStartTime;
    }

    public void setExpecteddIdleStartTime(Double expecteddIdleStartTime) {
        this.expecteddIdleStartTime = expecteddIdleStartTime;
    }

    public Double getEndTime() {
        return endTime;
    }

    public void setEndTime(Double endTime) {
        this.endTime = endTime;
    }
}
