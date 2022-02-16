package at.uibk.dps.di.scheduler;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Class representing the available resources.
 *
 * @author Stefan Pedratscher
 */
public class Resource {
    private class Space{
        double start;
        double end;

        public Space(double start, double end) {
            this.start = start;
            this.end = end;
        }

        public double getStart() {
            return start;
        }

        public void setStart(double start) {
            this.start = start;
        }

        public double getEnd() {
            return end;
        }

        public void setEnd(double end) {
            this.end = end;
        }
    }

    public List<String> view = new ArrayList<>();
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
    private List<Double> available;

    public List<Space> spaces;

    public void printSPaces(){
        for(Space s: spaces){
            System.out.println(s.start +" - " + s.getEnd() + ". " + getType());
        }
    }

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
    public Resource(final String type, final int totalNumInstances) {
        this.type = type;
        this.totalNumInstances = totalNumInstances;
        latencyGlobal = 0.0;
        latencyLocal = 0.0;
        available = new ArrayList<>();
        spaces = new ArrayList<>();
    }

    /**
     * Default constructor.
     *
     * @param type the type of the resource.
     * @param totalNumInstances total number of available instances.
     * @param latencyLocal  the latency from a resource of the same type.
     * @param latencyGlobal the latency from a resource of a different type.
     */
    public Resource(final String type, final int totalNumInstances, final double latencyLocal, final double latencyGlobal) {
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
     * @return the finish time of the task.
     */
    public Double setResource(final double possibleStart, final double taskDuration, final boolean prevOnSameResource) {

        // TODO
        if(getType().contains("Local")) {
            for (int i = 0; i < spaces.size(); i++) {
                final Double finishTime = prevOnSameResource ? possibleStart + latencyLocal + taskDuration : possibleStart + taskDuration + latencyGlobal + latencyLocal;
                if (spaces.get(i).getStart() <= possibleStart && spaces.get(i).getEnd() >= finishTime) {
                    System.out.println("----------------------------------------------------USING SPACE");
                    if(spaces.get(i).getStart() < possibleStart) {
                        spaces.add(new Space(spaces.get(i).getStart(), possibleStart));
                    }
                    if(spaces.get(i).getEnd() > finishTime) {
                        spaces.add(new Space(finishTime, spaces.get(i).getEnd()));
                    }
                    spaces.remove(i);
                    return finishTime;
                }
            }
        }


        // Iterate over all active instances
        for(int i = 0; i < available.size(); i++) {

            // Check if resource is available at the optimal start time
            if(available.get(i) <= possibleStart) {

                // TODO
                if(getType().contains("Local")) {
                    if (available.get(i) != possibleStart) {

                        System.out.println(" ... Adding space for " + getType() + ": " + available.get(i) + " to " + possibleStart);
                        spaces.add(new Space(available.get(i), possibleStart));
                    }
                }

                final Double finishTime = prevOnSameResource ? possibleStart + taskDuration + latencyLocal : possibleStart + taskDuration + latencyGlobal + latencyLocal;
                available.set(i, finishTime);
                return finishTime;
            }
        }

        // If we have more instances then currently set we can assign a fresh instance
        if(totalNumInstances > available.size()){
            final Double finishTime = prevOnSameResource ? possibleStart + taskDuration + latencyLocal : possibleStart + taskDuration + latencyGlobal + latencyLocal;
            available.add(finishTime);
            return finishTime;
        }

        // Calculate the minimal available time of all resource instances
        final double minimalTime = Collections.min(available);

        // Set the minimal instance on the resource
        final Double finishTime = prevOnSameResource ? minimalTime + taskDuration + latencyLocal : minimalTime + taskDuration + latencyGlobal + latencyLocal;
        available.set(available.indexOf(minimalTime), finishTime);
        return finishTime;
    }

    /**
     * Get the earliest start time possible for the resource.
     *
     * @param possibleStart the time when the node could potentially start.
     * @param prevOnSameResource true if the previous task was on the same resource.
     *
     *  @return the earliest start time possible for the resource.
     */
    public Double earliestStartTime(final double possibleStart, final boolean prevOnSameResource, final double duration){

        // If we have instances that are currently not set
        // we can start at the best possible time
        if(totalNumInstances > available.size()){
            return prevOnSameResource ? possibleStart + latencyLocal + duration : possibleStart + latencyGlobal + latencyLocal+ duration;
        }

        if(getType().contains("Local")) {
            for (int i = 0; i < spaces.size(); i++) {
                final Double finishTime = prevOnSameResource ? possibleStart + latencyLocal + duration : possibleStart + duration + latencyGlobal + latencyLocal;
                if (spaces.get(i).getStart() <= possibleStart && spaces.get(i).getEnd() >= finishTime) {
                    System.out.println("............ SPACE DETECTED AND SUITABLE ................ " + this.getType() + " " + (spaces.get(i).getEnd() - spaces.get(i).getStart())
                            + ", bc " + spaces.get(i).getStart() + " <= " + possibleStart + " && " + spaces.get(i).getEnd() + " >= " + finishTime);
                    return finishTime;
                }
            }
        }

        // Iterate over available resources and check if
        // node could start at best possible time
        for(final Double a: available){
            if(a <= possibleStart) {
                return prevOnSameResource ? possibleStart + duration + latencyLocal : possibleStart + duration + latencyGlobal + latencyLocal;
            }
        }

        // Return the minimal time on the resources
        return prevOnSameResource ? Collections.min(available) + duration + latencyLocal : Collections.min(available) + duration + latencyGlobal + latencyLocal;
    }

    /**
     * Get the maximum duration of a resource.
     *
     * @return the maximum duration of the resource.
     */
    public Double maxDuration(){
        return available.isEmpty() ? 0.0 : Collections.max(available);
    }

    /**
     * Getter and Setter
     */

    public String getType() {
        return type;
    }

    public void setType(final String type) {
        this.type = type;
    }

    public int getTotalNumInstances() {
        return totalNumInstances;
    }

    public void setTotalNumInstances(final int totalNumInstances) { this.totalNumInstances = totalNumInstances; }

    public double getLatencyLocal() {
        return latencyLocal;
    }

    public void setLatencyLocal(final double latencyLocal) {
        this.latencyLocal = latencyLocal;
    }

    public double getLatencyGlobal() {
        return latencyGlobal;
    }

    public void setLatencyGlobal(final double latencyGlobal) {
        this.latencyGlobal = latencyGlobal;
    }

    public List<Double> getAvailable() {
        return available;
    }

    public void setAvailable(List<Double> available) {
        this.available = available;
    }
}
