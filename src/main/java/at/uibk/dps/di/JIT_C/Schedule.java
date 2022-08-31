package at.uibk.dps.di.JIT_C;

import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

public class Schedule {

    Task task;
    Resource vm;
    String instance;
    double xst;
    double xft;

    public Schedule(Task task, Resource vm, String instance) {
        this.task = task;
        this.vm = vm;
        this.instance = instance;
    }

    public Schedule(Task task, Resource vm, String instance, double xst, double xft) {
        this.task = task;
        this.vm = vm;
        this.xst = xst;
        this.xft = xft;
        this.instance = instance;
    }

    public Schedule(Task task, Resource vm, double xst, double xft) {
        this.task = task;
        this.vm = vm;
        this.xst = xst;
        this.xft = xft;
    }

    public String getInstance() {
        return instance;
    }

    public void setInstance(String instance) {
        this.instance = instance;
    }

    public Task getTask() {
        return task;
    }

    public void setTask(Task task) {
        this.task = task;
    }

    public Resource getVm() {
        return vm;
    }

    public void setVm(Resource vm) {
        this.vm = vm;
    }

    public double getXst() {
        return xst;
    }

    public void setXst(double xst) {
        this.xst = xst;
    }

    public double getXft() {
        return xft;
    }

    public void setXft(double xft) {
        this.xft = xft;
    }
}
