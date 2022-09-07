package at.uibk.dps.di.schedulerV2;

import at.uibk.dps.di.properties.PropertyServiceScheduler;
import at.uibk.dps.ee.model.graph.EnactmentGraph;
import at.uibk.dps.ee.model.graph.EnactmentSpecification;
import at.uibk.dps.ee.model.graph.MappingsConcurrent;
import at.uibk.dps.ee.model.graph.ResourceGraph;
import jdk.javadoc.internal.doclets.toolkit.PropertyUtils;
import jdk.nashorn.internal.runtime.Property;
import net.sf.opendse.model.Communication;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class Scheduler {

    /**
     * Keeps track of the calculated ranks.
     */
    private final Map<Task, Double> mapRank;

    private final Map<Task, Double> mapRankInit;

    /**
     * Keeps track of task resource mappings.
     */
    private final Map<Task, ResourceV2> mapResource;

    private final Map<Task, Double> mapFT;

    private List<LatencyMapping> latencyMappings;

    /**
     * Default constructor.
     */
    public Scheduler() {
        this.mapRank = new ConcurrentHashMap<>();
        this.mapResource = new ConcurrentHashMap<>();
        this.mapFT = new ConcurrentHashMap<>();
        this.mapRankInit = new ConcurrentHashMap<>();
        this.latencyMappings = new ArrayList<>();
    }

    public Scheduler(List<LatencyMapping> latencyMappings) {
        this.mapRank = new ConcurrentHashMap<>();
        this.mapResource = new ConcurrentHashMap<>();
        this.mapFT = new ConcurrentHashMap<>();
        this.mapRankInit = new ConcurrentHashMap<>();
        this.latencyMappings = new ArrayList<>();
        this.latencyMappings.addAll(latencyMappings);
    }

    /**
     * Rank all tasks of the workflow using upwards rank.
     *
     * @param specification containing tasks to rank.
     */
    private void rankUpWards(Map<Task, Double> ranks, EnactmentSpecification specification, Collection<Task> startNodes) {

        EnactmentGraph eGraph = specification.getEnactmentGraph();

        // Stack containing nodes to check
        Stack<Task> nodeStack = new Stack<>();

        // Start nodes
        startNodes.forEach(nodeStack::push);

        // Continue until all tasks are ranked
        while (!nodeStack.isEmpty()) {
            Task node = nodeStack.pop();

            // Check if task node
            if(!(node instanceof Communication)) {

                Collection<Task> successorTaskNodes = GraphUtility.getSuccessorTaskNodes(specification.getEnactmentGraph(), node);

                // Get highest rank of successor task
                double successorRank = 0.0;
                for(Task successorTask: successorTaskNodes) {
                    if(ranks.containsKey(successorTask) && successorRank < ranks.get(successorTask)) {
                        successorRank = ranks.get(successorTask);
                    }
                }

                // Calculate and set rank
                double rank = successorRank + GraphUtility.getAvgDurationOnAllResources(specification, node);
                ranks.put(node, rank);
                node.setAttribute("rankUpwards", node.getId() + ": " + rank);
            }
            nodeStack.addAll(eGraph.getPredecessors(node));
        }
    }

    private void updateUpwards(Map<Task, Double> ranks, EnactmentSpecification specification) {

        EnactmentGraph eGraph = specification.getEnactmentGraph();

        // Stack containing nodes to check
        Stack<Task> nodeStack = new Stack<>();

        // Start nodes
        Collection<Task> leafNodes = GraphUtility.getLeafNodes(specification.getEnactmentGraph());
        leafNodes.forEach(nodeStack::push);

        // Continue until all tasks are ranked
        while (!nodeStack.isEmpty()) {
            Task node = nodeStack.pop();

            // Check if task node
            if(!(node instanceof Communication) && ranks.containsKey(node)) {

                Collection<Task> successorTaskNodes = GraphUtility.getSuccessorTaskNodes(specification.getEnactmentGraph(), node);

                // Get highest rank of successor task
                double updatedRank = 0.0;
                for(Task successorTask: successorTaskNodes) {
                    if(ranks.containsKey(successorTask) && updatedRank < (ranks.get(successorTask) - mapRankInit.get(successorTask))) {
                        updatedRank = (ranks.get(successorTask) - mapRankInit.get(successorTask));
                    }
                }

                // Calculate and set rank
                double rank = updatedRank + ranks.get(node);
                ranks.replace(node, rank);
            }
            nodeStack.addAll(eGraph.getPredecessors(node));
        }
    }

    /**
     * Dynamically update the rank.
     *
     * @param specification containing tasks to rank.
     */
    private void updateRank(Map<Task, Double> ranks, Map<Task, ResourceV2> resources, EnactmentSpecification specification, Task task, double rank) {

        Collection<Task> successorTaskNodes = GraphUtility.getSuccessorTaskNodes(specification.getEnactmentGraph(), task);

        for(Task t: successorTaskNodes) {
            //if(ranks.containsKey(t) && resources.containsKey(task)) {

                double maxLatency = 0.0;
                for (LatencyMapping lm : latencyMappings) {
                    if ((resources.get(task).getId().contains(lm.getNode1()) && resources.get(task).getId().contains(lm.getNode2()))) {
                        if(maxLatency < lm.getLatency()) {
                            maxLatency = lm.getLatency();
                        }
                    }
                }

                double newRank = ranks.get(t) + (rank - mapRankInit.get(task)) + maxLatency;

                ranks.replace(t, newRank);

                Collection<Task> pre = GraphUtility.getPredecessorTaskNodes(specification.getEnactmentGraph(), t);
                for(Task p: pre) {
                    if(!p.getId().equals(task.getId())) {
                        if (ranks.containsKey(p) && ranks.get(p) < ranks.get(t)) {
                            updateUpwards(ranks, specification);
                        }
                    }
                }

                //updateRank(ranks, resources, specification, t, ranks.get(t));
            //}
        }


        System.out.print("Updated ranks for: ");
        successorTaskNodes.forEach((t) -> System.out.print(t.getId() + ": " + ranks.get(t) + ", "));
        System.out.println();
    }

    /**
     * Sort tasks by descending order.
     *
     * @param rankedTasks tasks to sort.
     *
     * @return sorted ranks.
     */
    public Stack<Task> sort(Map<Task, Double> ranks, ArrayList<Task> rankedTasks, EnactmentSpecification specification, Map<Task, ResourceV2> resources){

        // Sort the given ranks
        rankedTasks.sort((o1, o2) -> {
            double rank1 = ranks.get(o1);
            double rank2 = ranks.get(o2);
            if (rank1 == rank2) {

                /*if(o1.getId().contains("Node6") && o2.getId().contains("Node7")) {
                    System.out.println(23);
                }

                double to1 = 0.0;
                for(Task p: GraphUtility.getPredecessorTaskNodes(specification.getEnactmentGraph(), o1)) {
                    to1 += resources.containsKey(p) ? resources.get(p).getAverageLatency() : 0.0;
                }
                double to2 = 0.0;
                for(Task p: GraphUtility.getPredecessorTaskNodes(specification.getEnactmentGraph(), o2)) {
                    to2 += resources.containsKey(p) ? resources.get(p).getAverageLatency() : 0.0;
                }
                if(to1 > to2) {
                    return 1;
                }else if (to1 < to2) {
                    return -1;
                }*/

                double to1 = 0.0;
                for(Task p: GraphUtility.getPredecessorTaskNodes(specification.getEnactmentGraph(), o1)) {
                    to1 += resources.containsKey(p) && resources.containsKey(o1) && resources.get(p).getId().equals(resources.get(o1).getId()) ? 1.0 : 0.0;
                }
                double to2 = 0.0;
                for(Task p: GraphUtility.getPredecessorTaskNodes(specification.getEnactmentGraph(), o2)) {
                    to2 += resources.containsKey(p) && resources.containsKey(o2) && resources.get(p).getId().equals(resources.get(o2).getId()) ? 1.0 : 0.0;
                }
                if(to1 > to2) {
                    return -1;
                }else if (to1 < to2) {
                    return 1;
                }
                for(Task p: GraphUtility.getPredecessorTaskNodes(specification.getEnactmentGraph(), o1)) {
                    if(p.getId().equals(o2.getId())) {
                        return -1;
                    }
                }
                for(Task p: GraphUtility.getPredecessorTaskNodes(specification.getEnactmentGraph(), o2)) {
                    if(p.getId().equals(o1.getId())) {
                        return 1;
                    }
                }


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

        // Temporary resource mappings, task stack, function finish time mappings
        Map<Task, ResourceV2> tmpMapResource = new ConcurrentHashMap<>(mapResource);
        Map<Task, Double> tmpMapFT = new ConcurrentHashMap<>(mapFT);
        Map<Task, Double> tmpMapRank = new ConcurrentHashMap<>(mapRank);
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
        if(rankedTask.getId().contains("7") && resource.getId().contains("Local")) {
            System.out.println("here");
        }
        // Temporarily assign task to resource
        double ft = resource.ftTask(rankedTask, pStart, true, tmpMapResource, true);
        tmpMapResource.put(rankedTask, resource);
        tmpMapFT.put(rankedTask, ft);
        //rankedTask.setAttribute(GraphUtility.counter + "_ft", ft + "_" + (resource.getId().contains("Local") ? "L" : "CLOUD"));

        double maxFT = ft;

        updateRank(tmpMapRank, tmpMapResource, specification, rankedTask, tmpMapRank.remove(rankedTask));
        taskStack = sort(tmpMapRank, new ArrayList<>(tmpMapRank.keySet()), specification, tmpMapResource);


        System.out.println("   Try task: " + rankedTask + " on " + (resource.getId().contains("Local") ? "L" : "CLOUD") + " with rank " + mapRank.get(rankedTask));

        while(!taskStack.empty()) {
            Task t = taskStack.pop();

            Collection<Task> predecessors = GraphUtility.getPredecessorTaskNodes(specification.getEnactmentGraph(), t);
            double possStart = 0.0;
            for(Task p: predecessors) {
                if(rankedTask.getId().contains("60") && resource.getId().contains("Local") && p.getId().contains("100")) {
                    System.out.println();
                }
                if(possStart < tmpMapFT.get(p)) {
                    possStart = tmpMapFT.get(p);
                }
            }

            ResourceV2 bestResource = resources.get(0);
            double bestFT = Double.MAX_VALUE;
            for(Mapping<Task, Resource> taskResourceMapping :specification.getMappings().getMappings(t)) {
                for(ResourceV2 r: resources) {
                    if (r.getId().equals(taskResourceMapping.getTarget().getId())) {
                        double tmpFT = r.ftTask(t, possStart, false, tmpMapResource, true);
                        if (tmpFT < bestFT) {
                            bestFT = tmpFT;
                            bestResource = r;
                        } else if (tmpFT == bestFT) {
                            // Grouping
                            Collection<Task> predecessorTaskNodes = GraphUtility.getPredecessorTaskNodes(specification.getEnactmentGraph(),
                                rankedTask);
                            for (Task p : predecessorTaskNodes) {
                                if (mapResource.get(p).getId().equals(resource.getId())) {
                                    bestFT = tmpFT;
                                    bestResource = resource;
                                }
                            }
                        }
                    }
                }
            }
            bestResource.ftTask(t, possStart, true, tmpMapResource, true);
            tmpMapResource.put(t, bestResource);
            tmpMapFT.put(t, bestFT);
            System.out.println("..handled task: " + t + " with rank " + tmpMapRank.get(t) + " on " + (bestResource.getId().contains("Local") ? "L" : "CLOUD") + " and FT: " + bestFT);
            t.setAttribute(GraphUtility.counter + "_ft_" + rankedTask + "_" + (resource.getId().contains("Local") ? "L" : "CLOUD"),    t.getId() + "_" + bestFT + "_" + (bestResource.getId().contains("Local") ? "L" : "CLOUD"));
            if(maxFT < bestFT) {
                maxFT = bestFT;
            }

            if(rankedTask.getId().contains("Node1") && resource.getId().contains("http") && t.getId().contains("Node4")) {
                System.out.println(2);
            }

            updateRank(tmpMapRank, tmpMapResource, specification, t, tmpMapRank.get(t));
            taskStack = sort(tmpMapRank, new ArrayList<>(tmpMapRank.keySet()), specification, tmpMapResource);
            tmpMapRank.remove(t);
            taskStack.remove(t);
            tmpMapRank.forEach((ta,r) -> ta.setAttribute(GraphUtility.counter + "_ranks", r + ":" + ta.getId()));
        }

        return maxFT;
    }

    private Double budgetLevelGlobal = 0.0;
    private Map<Task, Double> budgetLevel = new HashMap<>();
    private Map<Task, Double> taskCost = new HashMap<>();
    private Map<Task, Double> taskMinCost = new HashMap<>();
    private Map<Task, Double> taskMaxCost = new HashMap<>();

    private double calcTotal(Map<Task, Double> map){
        double total = 0.0;
        for(Task t: map.keySet()) {
            total += map.get(t);
        }
        return total;
    }

    private void setupCost(EnactmentSpecification specification, double budget) {
        EnactmentGraph eGraph = specification.getEnactmentGraph();
        for(Task t: eGraph.getVertices()) {
            if(!(t instanceof Communication)) {
                double min = Double.MAX_VALUE;
                double max = 0.0;
                for(Mapping<Task, Resource> map: specification.getMappings().getMappings(t)) {
                    double cost = map.getAttribute("Cost");
                    if(cost < min) {
                        min = cost;
                    }
                    if (cost > max) {
                        max = cost;
                    }
                }
                taskMinCost.put(t, min);
                taskMaxCost.put(t, max);
            }
        }
        budgetLevelGlobal = (budget - calcTotal(taskMinCost)) / (calcTotal(taskMaxCost) - calcTotal(taskMinCost));

        for(Task t: eGraph.getVertices()) {
            if(!(t instanceof Communication)) {
                budgetLevel.put(t, taskMinCost.get(t) + (taskMaxCost.get(t) - taskMinCost.get(t)) * budgetLevelGlobal);
            }
        }
        System.out.println(1);
    }

    private Double getCost(Task task, ResourceV2 resource, EnactmentSpecification specification){
        for(Mapping<Task, Resource> map: specification.getMappings().getMappings(task)) {
            if(map.getTarget().getId().equals(resource.getId())) {
                return map.getAttribute("Cost");
            }
        }
        //throw new Exception("Could not find cost for resource " + resource.getId());
        System.err.println("Could not find cost for resource " + resource.getId());
        return null;
    }

    /**
     * Actual scheduler.
     *
     * @param specification of the workflow to schedule.
     */
    public void schedule(EnactmentSpecification specification, double budget) {

        setupCost(specification, budget);

        // Get the resource and enactment graph and the vertices from the specification
        ResourceGraph rGraph = specification.getResourceGraph();

        // Transform to an internal representation for the resources
        List<ResourceV2> resources = new ArrayList<>();
        for(net.sf.opendse.model.Resource r: rGraph.getVertices()){
            resources.add(new ResourceV2(r.getId(), PropertyServiceScheduler.getInstances(r), specification, latencyMappings));
        }

        // Rank tasks initially with upwards rank
        Collection<Task> leafNodes = GraphUtility.getLeafNodes(specification.getEnactmentGraph());
        rankUpWards(mapRank, specification, leafNodes);
        rankUpWards(mapRankInit, specification, leafNodes);

        // Sort ranked tasks
        Stack<Task> rankedTaskStack = sort(mapRank, new ArrayList<>(mapRank.keySet()), specification, mapResource);

        // Continue while there are ranked tasks
        // TODO do this dynamic (once task is ready do a step in the loop)
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

            if(specification.getMappings().getMappings(rankedTask).isEmpty()) {
                System.err.println("Could not find suitable mapping for task " + rankedTask.getId());
            }

            // Iterate over all possible resources of selected task
            for(Mapping<Task, Resource> taskResourceMapping :specification.getMappings().getMappings(rankedTask)) {
                for(ResourceV2 resource: resources) {
                    if (resource.getId().equals(taskResourceMapping.getTarget().getId())) {
                        double tmpFT = heft(specification, resources, resource, rankedTaskStack, rankedTask, possStart);
                        if (tmpFT < bestFT) {
                            bestFT = tmpFT;
                            bestResource = resource;
                        } else if (tmpFT == bestFT) {
                            // Grouping
                            Collection<Task> predecessorTaskNodes = GraphUtility.getPredecessorTaskNodes(specification.getEnactmentGraph(),
                                rankedTask);
                            for (Task p : predecessorTaskNodes) {
                                if (mapResource.get(p).getId().equals(resource.getId())) {
                                    bestFT = tmpFT;
                                    bestResource = resource;
                                }
                            }
                        }
                    }
                }
            }
            GraphUtility.counter++;

            double cost = getCost(rankedTask, bestResource, specification);
            double currentCost = taskCost.values().stream().mapToDouble(Double::doubleValue).sum();
            if(cost > budgetLevel.get(rankedTask) && currentCost + cost + calcTotal(taskMinCost) - taskMinCost.get(rankedTask) > budget) {
                    // cannot take this one
                System.err.println("Cannot take vorgesehenen one");
                Mapping<Task, Resource> mapToDelete = null;
                Set<Mapping<Task, Resource>> mappings = specification.getMappings().getMappings(rankedTask);
                for(Mapping<Task, Resource> map: mappings) {
                    if(map.getTarget().getId().equals(bestResource.getId())) {
                        mapToDelete = map;
                    }
                }
                specification.getMappings().removeMapping(mapToDelete);
                rankedTaskStack.push(rankedTask);
            } else {
                double ft = bestResource.ftTask(rankedTask, possStart, true, mapResource, false);
                mapResource.put(rankedTask, bestResource);
                mapFT.put(rankedTask, ft);
                System.out.println("Fixed task " + rankedTask + " on " + bestResource.getId() + " with FT=" + ft);
                rankedTask.setAttribute("FINAL_FT", ft + "_" + (bestResource.getId().contains("Local") ? "L" : "CLOUD"));

                updateRank(mapRank, mapResource, specification, rankedTask, mapRank.get(rankedTask));
                mapRank.remove(rankedTask);
                rankedTaskStack = sort(mapRank, new ArrayList<>(mapRank.keySet()), specification, mapResource);
                //mapRank.forEach((t,r) -> t.setAttribute(GraphUtility.counter + "_rank:", r));

                taskMinCost.remove(rankedTask);
                taskCost.put(rankedTask, cost);
            }
        }
    }


}
