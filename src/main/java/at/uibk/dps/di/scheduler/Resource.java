package at.uibk.dps.di.scheduler;

import java.util.ArrayList;
import java.util.Collections;

/**
 * Class representing the available resources.
 *
 * @author Stefan Pedratscher
 */
public class Resource {

    /**
     * The type of the resource.
     */
    private String type;

    /**
     * The number of instances available for this resource.
     */
    private int totalNumInstances;

    /**
     * The time when the resources are available.
     */
    private ArrayList<Double> available;

    /**
     * The latency from a resource of the same type.
     */
    private double latencyLocal;

    /**
     * The latency from a resource of a different type.
     */
    private double latencyGlobal;

    /**
     * Default constructor.
     *
     * @param type the type of the resource.
     * @param totalNumInstances total number of available instances.
     */
    public Resource(String type, int totalNumInstances) {
        this.type = type;
        this.totalNumInstances = totalNumInstances;
        latencyGlobal = 0.0;
        latencyLocal = 0.0;
        available = new ArrayList<>();
    }

    /**
     * Default constructor.
     *
     * @param type the type of the resource.
     * @param totalNumInstances total number of available instances.
     * @param latencyLocal  the latency from a resource of the same type.
     * @param latencyGlobal the latency from a resource of a different type.
     */
    public Resource(String type, int totalNumInstances, double latencyLocal, double latencyGlobal) {
        this(type, totalNumInstances);
        this.latencyLocal = latencyLocal;
        this.latencyGlobal = latencyGlobal;
    }

    /**
     * Set the resource usage.
     *
     * @param possibleStart the time when the node could potentially start.
     * @param taskDuration the duration of the node.
     * @param prevOnSameResource true if the previous task was on the same resource.
     *
     */
    public void setResource(double possibleStart, double taskDuration, boolean prevOnSameResource) {

        // Iterate over all active instances
        for(int i = 0; i < available.size(); i++) {

            // Check if resource is available at the optimal start time
            if(available.get(i) <= possibleStart) {
                available.set(i, prevOnSameResource ? possibleStart + taskDuration + latencyLocal : possibleStart + taskDuration + latencyGlobal);
                return;
            }
        }

        // If we have more instances then currently set we can assign a fresh instance
        if(totalNumInstances > available.size()){
            available.add(prevOnSameResource ? possibleStart + taskDuration + latencyLocal : possibleStart + taskDuration + latencyGlobal);
            return;
        }

        // Calculate the minimal available time of all resource instances
        double minimalTime = Collections.min(available);

        // Set the minimal instance on the resource
        available.set(
            available.indexOf(minimalTime),
            prevOnSameResource ? minimalTime + taskDuration + latencyLocal : minimalTime + taskDuration + latencyGlobal
        );
    }

    /**
     * Get the earliest start time possible for the resource.
     *
     * @param possibleStart the time when the node could potentially start.
     * @param prevOnSameResource true if the previous task was on the same resource.
     *
     *  @return the earliest start time possible for the resource.
     */
    public Double earliestStartTime(double possibleStart, boolean prevOnSameResource){

        // If we have instances that are currently not set we can start at the best possible time
        if(totalNumInstances > available.size()){
            return prevOnSameResource ? possibleStart + latencyLocal : possibleStart + latencyGlobal;
        }

        // Iterate over available resources and check if node could start at best possible time
        for(Double a: available){
            if(a <= possibleStart) {
                return prevOnSameResource ? possibleStart + latencyLocal : possibleStart + latencyGlobal;
            }
        }

        // Return the minimal time on the resources
        return prevOnSameResource ? Collections.min(available) + latencyLocal : Collections.min(available) + latencyGlobal;
    }

    /**
     * Getter and Setter
     */

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    public int getTotalNumInstances() {
        return totalNumInstances;
    }

    public void setTotalNumInstances(int totalNumInstances) {
        this.totalNumInstances = totalNumInstances;
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
