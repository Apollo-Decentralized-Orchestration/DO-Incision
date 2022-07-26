package at.uibk.dps.di.schedulerV2;

import at.uibk.dps.di.properties.PropertyServiceScheduler;
import at.uibk.dps.ee.model.graph.EnactmentGraph;
import at.uibk.dps.ee.model.graph.EnactmentSpecification;
import at.uibk.dps.ee.model.graph.ResourceGraph;
import net.sf.opendse.model.Communication;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;
import org.apache.commons.lang3.SerializationUtils;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Scheduler {

    /**
     * Keeps track of the calculated ranks.
     */
    private final Map<Task, Double> mapRank;

    /**
     * Keeps track of task resource mappings.
     */
    private final Map<Task, ResourceV2> mapResource;

    private final Map<Task, Double> mapFT;

    /**
     * Default constructor.
     */
    public Scheduler() {
        this.mapRank = new ConcurrentHashMap<>();
        this.mapResource = new ConcurrentHashMap<>();
        this.mapFT = new ConcurrentHashMap<>();
    }

    /**
     * Rank all tasks of the workflow using upwards rank.
     *
     * @param specification containing tasks to rank.
     */
    private void rankUpWards(EnactmentSpecification specification) {

        EnactmentGraph eGraph = specification.getEnactmentGraph();

        // Stack containing nodes to check
        Stack<Task> nodeStack = new Stack<>();

        // Add all leaf notes to stack
        GraphUtility.getLeafNodes(specification.getEnactmentGraph()).forEach(nodeStack::push);

        // Continue until all tasks are ranked
        while (!nodeStack.isEmpty()) {
            Task node = nodeStack.pop();

            // Check if task node
            if(!(node instanceof Communication)) {

                Collection<Task> successorTaskNodes = GraphUtility.getSuccessorTaskNodes(specification.getEnactmentGraph(), node);

                // Get highest rank of successor task
                double successorRank = 0.0;
                for(Task successorTask: successorTaskNodes) {
                    if(mapRank.containsKey(successorTask) && successorRank < mapRank.get(successorTask)) {
                        successorRank = mapRank.get(successorTask);
                    }
                }

                // Calculate and set rank
                double rank = successorRank + GraphUtility.getAvgDurationOnAllResources(specification, node);
                mapRank.put(node, rank);
                node.setAttribute("rankUpwards", node.getId() + ": " + rank);
            }
            nodeStack.addAll(eGraph.getPredecessors(node));
        }
    }

    /**
     * Sort tasks by descending order.
     *
     * @param rankedTasks tasks to sort.
     *
     * @return sorted ranks.
     */
    public Stack<Task> sort(ArrayList<Task> rankedTasks){

        // Sort the given ranks
        rankedTasks.sort((o1, o2) -> {
            double rank1 = mapRank.get(o1);
            double rank2 = mapRank.get(o2);
            if (rank1 == rank2) {
                return 0;
            }
            return rank1 < rank2 ? -1 : 1;
        });

        Stack<Task> rankedTaskStack = new Stack<>();
        rankedTaskStack.addAll(rankedTasks);
        return rankedTaskStack;
    }

    private double heft(EnactmentSpecification specification,
        List<ResourceV2> inputResources, ResourceV2 inputResource,
        Stack<Task> rankedTaskStack, Task rankedTask,
        double pStart) {

        if(GraphUtility.counter == 2) {
            System.out.println(2);
        }

        // Temporary resource mappings, task stack, function finish time mappings
        Map<Task, ResourceV2> tmpMapResource = new ConcurrentHashMap<>(mapResource);
        Map<Task, Double> tmpMapFT = new ConcurrentHashMap<>(mapFT);
        Stack<Task> taskStack = new Stack<>();
        taskStack.addAll(rankedTaskStack);

        // Temporarily copy resources and resource
        ResourceV2 resource = null;
        List<ResourceV2> resources = new ArrayList<>();
        for(ResourceV2 r: inputResources) {
            ResourceV2 cpy = r.copy();
            resources.add(cpy);
            if(r.getId().equals(inputResource.getId())) {
                resource = cpy;
            }
        }

        // Temporarily assign task to resource
        double ft = resource.ftTask(rankedTask, pStart, true, tmpMapResource);
        tmpMapResource.put(rankedTask, resource);
        tmpMapFT.put(rankedTask, ft);
        rankedTask.setAttribute(GraphUtility.counter + "_ft", ft + "_" + resource.getId());

        double maxFT = 0.0;

        while(!taskStack.empty()) {
            Task t = taskStack.pop();

            Collection<Task> predecessors = GraphUtility.getPredecessorTaskNodes(specification.getEnactmentGraph(), t);
            double possStart = 0.0;
            for(Task p: predecessors) {
                if(possStart < tmpMapFT.get(p)) {
                    possStart = tmpMapFT.get(p);
                }
            }

            ResourceV2 bestResource = resources.get(0);
            double bestFT = Double.MAX_VALUE;
            for(ResourceV2 r: resources) {
                double tmpFT = r.ftTask(t, possStart, false, tmpMapResource);
                if(tmpFT < bestFT) {
                    bestFT = tmpFT;
                    bestResource = r;
                }
            }
            bestResource.ftTask(t, possStart, true, tmpMapResource);
            tmpMapResource.put(t, bestResource);
            tmpMapFT.put(t, bestFT);
            t.setAttribute(GraphUtility.counter + "_ft", bestFT + "_" + bestResource.getId());
            if(maxFT < bestFT) {
                maxFT = bestFT;
            }
        }

        return maxFT;
    }

    /**
     * Actual scheduler.
     *
     * @param specification of the workflow to schedule.
     */
    public void schedule(EnactmentSpecification specification) {

        // Get the resource and enactment graph and the vertices from the specification
        ResourceGraph rGraph = specification.getResourceGraph();
        EnactmentGraph eGraph = specification.getEnactmentGraph();

        // Transform to an internal representation for the resources
        List<ResourceV2> resources = new ArrayList<>();
        for(net.sf.opendse.model.Resource r: rGraph.getVertices()){
            resources.add(new ResourceV2(r.getId(), PropertyServiceScheduler.getInstances(r), PropertyServiceScheduler.getLatencyLocal(r), PropertyServiceScheduler.getLatencyGlobal(r), specification));
        }

        // Rank tasks initially with upwards rank
        rankUpWards(specification);

        // Sort ranked tasks
        Stack<Task> rankedTaskStack = sort(new ArrayList<>(mapRank.keySet()));

        // Continue while there are ranked tasks
        while(!rankedTaskStack.isEmpty()) {

            Task rankedTask = rankedTaskStack.pop();

            Collection<Task> predecessors = GraphUtility.getPredecessorTaskNodes(specification.getEnactmentGraph(), rankedTask);
            double possStart = 0.0;
            for(Task p: predecessors) {
                if(possStart < mapFT.get(p)) {
                    possStart = mapFT.get(p);
                }
            }

            ResourceV2 bestResource = resources.get(0);
            double bestFT = Double.MAX_VALUE;

            // Iterate over all possible resources
            for(ResourceV2 resource: resources) {

                double tmpFT = heft(specification, resources, resource, rankedTaskStack, rankedTask, possStart);
                GraphUtility.counter++;
                if(tmpFT < bestFT) {
                    bestFT = tmpFT;
                    bestResource = resource;
                }
            }
            double ft = bestResource.ftTask(rankedTask, possStart, true, mapResource);
            mapResource.put(rankedTask, bestResource);
            mapFT.put(rankedTask, ft);
            System.out.println("Fixed task " + rankedTask + " on " + bestResource.getId() + " with FT=" + ft);
            rankedTask.setAttribute("FINAL_FT", ft + "_" + bestResource.getId());
        }
    }
}
