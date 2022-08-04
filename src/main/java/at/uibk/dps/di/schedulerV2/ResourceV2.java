package at.uibk.dps.di.schedulerV2;

import at.uibk.dps.di.scheduler.Resource;
import at.uibk.dps.ee.model.graph.EnactmentSpecification;
import net.sf.opendse.model.Task;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class ResourceV2 {

    private String id;
    private int instances;
    private double latencyLocal;
    private double latencyGlobal;
    private EnactmentSpecification enactmentSpecification;

    /**
     * Time when resource instances are available.
     */
    private List<Double> available;

    private List<LatencyMapping> latencyMappings;


    ResourceV2(String id, int instances, double latencyLocal, double latencyGlobal,
        EnactmentSpecification enactmentSpecification,  List<LatencyMapping> latencyMappings) {
        this.id = id;
        this.instances = instances;
        this.latencyLocal = latencyLocal;
        this.latencyGlobal = latencyGlobal;
        this.enactmentSpecification = enactmentSpecification;
        this.available = new ArrayList<>();
        this.latencyMappings = latencyMappings;
    }

    ResourceV2(String id, int instances,
        EnactmentSpecification enactmentSpecification, List<LatencyMapping> latencyMappings) {
        this.id = id;
        this.instances = instances;
        this.enactmentSpecification = enactmentSpecification;
        this.available = new ArrayList<>();
        this.latencyMappings = latencyMappings;
    }

    double ftTask(Task task, double possibleStart, boolean fix, Map<Task, ResourceV2> mapResource, boolean longTerm) {

        // Get duration of function on specific resource
        double duration = GraphUtility.getTaskDurationOnResource(enactmentSpecification, task, this);

        // Finish time of the task
        double ft = possibleStart + duration;

        // Check if at least one previous task was on the same rsource
        Collection<Task> predecessors = GraphUtility.getPredecessorTaskNodes(enactmentSpecification.getEnactmentGraph(), task);

        double maxLatency = 0.0;
        //boolean prevOnSameResource = false;
        for(Task p: predecessors) {
            if(!longTerm){
                for(LatencyMapping latencyMapping: latencyMappings) {
                    if((this.getId().contains(latencyMapping.getNode1()) && mapResource.get(p).getId().contains(latencyMapping.getNode2()))
                        || this.getId().contains(latencyMapping.getNode2()) && mapResource.get(p).getId().contains(latencyMapping.getNode1())){
                        if(maxLatency < latencyMapping.getLatency()) {
                            maxLatency = latencyMapping.getLatency();
                        }
                    }
                }
            } else {
                for (LatencyMapping lm : latencyMappings) {
                    if ((this.getId().contains(lm.getNode1()) && this.getId().contains(lm.getNode2()))) {
                        if(maxLatency < lm.getLatency()) {
                            maxLatency = lm.getLatency();
                        }
                    }
                }
            }
        }
        if(predecessors.size() == 0) {
            if(this.getId().contains("aws")) {
                maxLatency = 500.0;
            }
        }

        ft+=maxLatency;
        /*
        if(prevOnSameResource) {
            ft += latencyLocal;
        } else {
            ft += latencyGlobal;
        }*/


        // TODO reuse always resource is possible, maybe adapt?
        // Check if available instance has optimal start time (reuse same resource if possible)
        int bestAlternativeIndex = 0;
        for(int i = 0; i < available.size(); i++) {
            if(possibleStart > available.get(i)) {
                if(fix) {
                    available.set(i, ft);
                }
                return ft;
            } else {
                if(available.get(i) < available.get(bestAlternativeIndex)) {
                    bestAlternativeIndex = i;
                }
            }
        }

        // Check if another instance available
        boolean instanceAvail = available.size() < instances;

        // Check if new instance is available
        if(instanceAvail) {
            if(fix) {
                available.add(ft);
            }
            return ft;
        }

        // Set best other alternative resource
        double bestAlternativeFT = available.get(bestAlternativeIndex) + duration + maxLatency;
        if(fix) {
            available.set(bestAlternativeIndex, bestAlternativeFT);
        }
        return bestAlternativeFT;
    }

    public double getAverageLatency () {
        return (this.latencyGlobal + this.latencyLocal) / 2.0;
    }

    public ResourceV2 copy() {
        ResourceV2 copy = new ResourceV2(this.id, this.instances, this.latencyLocal, this.latencyGlobal, this.enactmentSpecification, this.latencyMappings);
        copy.available = new ArrayList<>(this.available);
        return copy;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public double getLatencyLocal() {
        return latencyLocal;
    }

    public void setLatencyLocal(double latencyLocal) {
        this.latencyLocal = latencyLocal;
    }

    public double getLatencyGlobal() {
        return latencyGlobal;
    }

    public void setLatencyGlobal(double latencyGlobal) {
        this.latencyGlobal = latencyGlobal;
    }
}
