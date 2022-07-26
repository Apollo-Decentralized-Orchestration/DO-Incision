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

    ResourceV2(String id, int instances, double latencyLocal, double latencyGlobal,
        EnactmentSpecification enactmentSpecification) {
        this.id = id;
        this.instances = instances;
        this.latencyLocal = latencyLocal;
        this.latencyGlobal = latencyGlobal;
        this.enactmentSpecification = enactmentSpecification;
        this.available = new ArrayList<>();
    }

    double ftTask(Task task, double possibleStart, boolean fix, Map<Task, ResourceV2> mapResource) {

        // Get duration of function on specific resource
        double duration = GraphUtility.getTaskDurationOnResource(enactmentSpecification, task, this);

        // Finish time of the task
        double ft = possibleStart + duration;

        // Check if at least one previous task was on the same rsource
        Collection<Task> predecessors = GraphUtility.getPredecessorTaskNodes(enactmentSpecification.getEnactmentGraph(), task);
        boolean prevOnSameResource = false;
        for(Task p: predecessors) {
            if(mapResource.get(p).getId().equals(this.getId())) {
                prevOnSameResource = true;
                break;
            }
        }

        if(prevOnSameResource) {
            ft += latencyLocal;
        } else {
            ft += latencyGlobal;
        }

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
        double bestAlternativeFT = available.get(bestAlternativeIndex) + duration + (prevOnSameResource ? latencyLocal : latencyGlobal);
        if(fix) {
            available.set(bestAlternativeIndex, bestAlternativeFT);
        }
        return bestAlternativeFT;
    }

    public ResourceV2 copy() {
        ResourceV2 copy = new ResourceV2(this.id, this.instances, this.latencyLocal, this.latencyGlobal, this.enactmentSpecification);
        copy.available = new ArrayList<>(this.available);
        return copy;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }
}
