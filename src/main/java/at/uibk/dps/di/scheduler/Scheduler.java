package at.uibk.dps.di.scheduler;

import at.uibk.dps.di.incision.Utility;
import at.uibk.dps.di.properties.PropertyServiceScheduler;
import at.uibk.dps.ee.model.graph.*;
import at.uibk.dps.ee.model.properties.PropertyServiceData;
import net.sf.opendse.model.Communication;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Task;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Class to schedule tasks on resources. Represents a modified HEFT algorithm.
 *
 * @author Stefan Pedratscher
 */
public class Scheduler {

    /**
     * Keeps track of the calculated ranks.
     */
    public final Map<Task, Double> mapRank;

    /**
     * Keeps track of the calculated finish times.
     */
    private final Map<Task, Double> mapFinishTime;

    /**
     * Keeps track of the set resources.
     */
    public final Map<Task, Resource> mapResource;

    /**
     * Default constructor
     */
    public Scheduler(){
        mapRank = new HashMap<>();
        mapFinishTime = new HashMap<>();
        mapResource = new HashMap<>();
    }

    /**
     * Get all leaf nodes (nodes containing attribute Leaf) in
     * an {@link EnactmentGraph}.
     *
     * @param eGraph the graph to check for leaf nodes.
     *
     * @return a collection of leaf nodes.
     */
    private Collection<Task> getLeafNodes(EnactmentGraph eGraph) {
        return eGraph.getVertices()
            .stream()
            .filter(task -> task instanceof Communication && PropertyServiceData.isLeaf(task))
            .collect(Collectors.toList());
    }

    /**
     * Jump over communication nodes and return previous task nodes.
     *
     * @param eGraph the {@link EnactmentGraph} to inspect.
     * @param node to get predecessor tasks from.
     *
     * @return list of predecessor task nodes.
     */
    public Collection<Task> getPredecessorTaskNodes(EnactmentGraph eGraph, Task node) {
        if(node instanceof Communication) {

            // The predecessor of a communication node is a task node
            return eGraph.getPredecessors(node);
        } else {

            // The predecessor of a task node is a communication node (which predecessor is a task node)
            Collection<Task> predecessorTasks = new ArrayList<>();
            eGraph.getPredecessors(node).forEach((pS) -> predecessorTasks.addAll(eGraph.getPredecessors(pS)));
            return predecessorTasks;
        }
    }

    /**
     * Jump over communication nodes and return next task nodes.
     *
     * @param eGraph the {@link EnactmentGraph} to inspect.
     * @param node to get successor tasks from.
     *
     * @return list of successor task nodes.
     */
    private Collection<Task> getSuccessorTaskNodes(EnactmentGraph eGraph, Task node) {
        if(node instanceof Communication) {

            // The successor of a communication node is a task node
            return eGraph.getSuccessors(node);
        } else {

            // The successor of a task node is a communication node (which successor is a task node)
            Collection<Task> successorTasks = new ArrayList<>();
            eGraph.getSuccessors(node).forEach((pS) -> successorTasks.addAll(eGraph.getSuccessors(pS)));
            return successorTasks;
        }
    }

    /**
     * Calculate the rank of a given task.
     *
     * @param currentTaskRank the rank of the current task.
     * @param predecessor the predecessor of the current task.
     * @param mappings the task mappings.
     * @param rankedTasks the ranked tasks.
     *
     * @return the rank of the current task.
     */
    private double calcRank(double currentTaskRank, Task predecessor, MappingsConcurrent mappings,
        ArrayList<Task> rankedTasks){

        // Check if predecessor node is a task node
        if (!(predecessor instanceof Communication)) {

            Set<Mapping<Task, net.sf.opendse.model.Resource>> predecessorMappings = mappings.getMappings(predecessor);

            // Check if duration attribute is specified
            if (predecessorMappings.isEmpty()) {
                throw new IllegalArgumentException(
                    "Node " + predecessor.getId() + " has no function duration");
            }

            // Get the duration of the predecessor task node
            double duration = predecessorMappings.stream()
                .mapToDouble(PropertyServiceScheduler::getDuration).sum() / predecessorMappings.size();

            // Represents the rank of the predecessor task node
            double rank = duration + currentTaskRank;

            // If predecessor does not contain rank set it, otherwise select the bigger rank
            if (!mapRank.containsKey(predecessor) || (mapRank.containsKey(predecessor) && rank > mapRank.get(predecessor))) {
                mapRank.put(predecessor, rank);
            }

            // Add task node to list of ranked task nodes
            if (!rankedTasks.contains(predecessor)) {
                rankedTasks.add(predecessor);
            }

            // Remember rank of the task
            currentTaskRank = mapRank.get(predecessor);
        }

        return currentTaskRank;
    }


    private double calcRank2(double currentTaskRank, Task predecessor, MappingsConcurrent mappings,
        ArrayList<Task> rankedTasks, List<Task> toConsider){

        // Check if predecessor node is a task node
        if (!(predecessor instanceof Communication)) {

            Set<Mapping<Task, net.sf.opendse.model.Resource>> predecessorMappings = mappings.getMappings(predecessor);

            // Check if duration attribute is specified
            if (predecessorMappings.isEmpty()) {
                throw new IllegalArgumentException(
                    "Node " + predecessor.getId() + " has no function duration");
            }

            // Get the duration of the predecessor task node
            double duration = predecessorMappings.stream()
                .mapToDouble(PropertyServiceScheduler::getDuration).sum() / predecessorMappings.size();

            // Represents the rank of the predecessor task node
            double rank = duration + currentTaskRank;

            // If predecessor does not contain rank set it, otherwise select the bigger rank
            if (!mapRank.containsKey(predecessor) || (mapRank.containsKey(predecessor) && rank > mapRank.get(predecessor))) {
                mapRank.put(predecessor, rank);
            }

            // Add task node to list of ranked task nodes
            if (!rankedTasks.contains(predecessor) && toConsider.contains(predecessor)) {
                rankedTasks.add(predecessor);
            }

            // Remember rank of the task
            currentTaskRank = mapRank.get(predecessor);
        }

        return currentTaskRank;
    }

    /**
     * Rank the tasks based on upward rank.
     *
     * @param specification containing the tasks to rank.
     *
     * @return list of ranked tasks.
     */
    public ArrayList<Task> rank(EnactmentSpecification specification) {

        EnactmentGraph eGraph = specification.getEnactmentGraph();
        MappingsConcurrent mappings = specification.getMappings();

        ArrayList<Task> rankedTasks = new ArrayList<>();

        // Stack containing nodes to check
        Stack<AbstractMap.SimpleEntry<Task, Double>> nodeStack = new Stack<>();
        getLeafNodes(eGraph).forEach(node -> nodeStack.push(new AbstractMap.SimpleEntry<>(node, 0.0)));

        // Continue until all tasks are ranked
        while (!nodeStack.isEmpty()) {

            AbstractMap.SimpleEntry<Task, Double> current = nodeStack.pop();

            // Get predecessors of the current task on the stack
            Collection<Task> predecessorNodes = eGraph.getPredecessors(current.getKey());

            // Iterate over all predecessor nodes
            for(Task predecessor: predecessorNodes) {

                double currentTaskRank = calcRank(current.getValue(), predecessor, mappings, rankedTasks);

                // Add predecessor and its rank to the stack
                nodeStack.add(new AbstractMap.SimpleEntry<>(predecessor, currentTaskRank));
            }
        }

        return rankedTasks;
    }

    public ArrayList<Task> rank2(List<Task> tasks, EnactmentSpecification specification) {

        EnactmentGraph eGraph = specification.getEnactmentGraph();
        MappingsConcurrent mappings = specification.getMappings();

        ArrayList<Task> rankedTasks = new ArrayList<>();

        // Stack containing nodes to check
        Stack<AbstractMap.SimpleEntry<Task, Double>> nodeStack = new Stack<>();
        getLeafNodes(eGraph).forEach(node -> nodeStack.push(new AbstractMap.SimpleEntry<>(node, 0.0)));

        // Continue until all tasks are ranked
        while (!nodeStack.isEmpty()) {

            AbstractMap.SimpleEntry<Task, Double> current = nodeStack.pop();

            // Get predecessors of the current task on the stack
            Collection<Task> predecessorNodes = eGraph.getPredecessors(current.getKey());

            // Iterate over all predecessor nodes
            for(Task predecessor: predecessorNodes) {

                double currentTaskRank = calcRank2(current.getValue(), predecessor, mappings, rankedTasks, tasks);

                // Add predecessor and its rank to the stack
                nodeStack.add(new AbstractMap.SimpleEntry<>(predecessor, currentTaskRank));
            }
        }

        rankedTasks.sort((o1, o2) -> {
            double rank1 = mapRank.get(o1);
            double rank2 = mapRank.get(o2);
            if (rank1 == rank2) {
                return 0;
            }
            return rank1 < rank2 ? -1 : 1;
        });


        return rankedTasks;
    }

    public ArrayList<Task> rankAndSort(EnactmentSpecification specification) {

        // Rank tasks
        ArrayList<Task> rankedTasks = rank(specification);

        // Sort ranked tasks
        rankedTasks.sort((o1, o2) -> {
            double rank1 = mapRank.get(o1);
            double rank2 = mapRank.get(o2);
            if (rank1 == rank2) {
                return 0;
            }
            return rank1 < rank2 ? 1 : -1;
        });

        return rankedTasks;
    }

    public Task fixCut(Stack<Task> stack, Set<Task> cut, EnactmentGraph eGraph, Resource currentResource,
        Task prev, Stack<Task> taskStack, boolean getPredecessor){

        // While there are predecessors check if they are on the same resource
        while (!stack.isEmpty()) {
            Task task = stack.pop();

            // Check if on the same resource
            if (mapResource.get(task).getType().equals(currentResource.getType())) {

                // Get other predecessors to check if they are on same resource
                if(getPredecessor){
                    stack.addAll(getPredecessorTaskNodes(eGraph, task));
                } else {
                    stack.addAll(getSuccessorTaskNodes(eGraph, task));
                }

                // Remove predecessor from nodes to check since it is already checked
                taskStack.remove(task);

                // Remember previous node
                prev = task;
            } else {

                // Fix top cut
                cut.addAll(eGraph.getPredecessors(prev));
            }
        }

        return prev;
    }

    /**
     * Extract the cuts from the {@link EnactmentGraph} knowing where which task should run.
     *
     * @param eGraph the enactment graph.
     * @param rankedTasks the ranked task list.
     *
     * @return list of proposed cuts.
     */
    public List<Cut> extractCuts(EnactmentGraph eGraph, ArrayList<Task> rankedTasks) {
        List<Cut> proposedCuts = new ArrayList<>();

        // Stack containing all tasks to be checked
        Stack<Task> taskStack = new Stack<>();
        taskStack.addAll(rankedTasks);

        // Iterate over all tasks
        while(!taskStack.isEmpty()) {

            // Get a task node from the stack
            Task current = taskStack.pop();

            // Determine resource on which task node will be executed
            Resource currentResource = mapResource.get(current);

            // Check if it is the local resource
            if (!currentResource.getType().equals(Utility.ENGINE)) {
                Set<Task> topCut = new HashSet<>();
                Set<Task> bottomCut = new HashSet<>();

                // Create stack for the predecessor task nodes
                Stack<Task> stackPredecessor = new Stack<>();
                stackPredecessor.addAll(getPredecessorTaskNodes(eGraph, current));
                Task prev = current;

                // Fix the cut
//                prev = fixCut(stackPredecessor, topCut, eGraph, currentResource, prev, taskStack, true);

                // While there are predecessors check if they are on the same resource
                while (!stackPredecessor.isEmpty()) {
                    Task pre = stackPredecessor.pop();

                    // Check if on the same resource
                    if (mapResource.get(pre).getType().equals(currentResource.getType())) {

                        // Get other predecessors to check if they are on same resource
                        stackPredecessor.addAll(getPredecessorTaskNodes(eGraph, pre));

                        // Remove predecessor from nodes to check since it is already checked
                        taskStack.remove(pre);

                        // Remember previous node
                        prev = pre;
                    } else {

                        // Fix top cut
                        topCut.addAll(eGraph.getPredecessors(prev));
                    }

                }

                // Create stack for the successor task nodes
                Stack<Task> stackSuccessors = new Stack<>();
                stackSuccessors.addAll(getSuccessorTaskNodes(eGraph, current));
                prev = current;

//                prev = fixCut(stackSuccessors, bottomCut, eGraph, currentResource, prev, taskStack, false);

                // While there are successors check if they are on the same resource
                while (!stackSuccessors.isEmpty()) {
                    Task suc = stackSuccessors.pop();

                    // Check if on the same resource
                    if (mapResource.get(suc).getType().equals(currentResource.getType())) {

                        // Get other successors to check if they are on same resource
                        stackSuccessors.addAll(getSuccessorTaskNodes(eGraph, suc));

                        // Remove successor from nodes to check since it is already checked
                        taskStack.remove(suc);

                        // Remember previous node
                        prev = suc;
                    } else {

                        // Fix top cut
                        bottomCut.addAll(eGraph.getSuccessors(prev));
                    }
                }

                proposedCuts.add(new Cut(topCut, bottomCut));
            }
        }
        return proposedCuts;
    }

    /**
     * Evaluate and schedule the tasks to the resources.
     *
     * @param specification the {@link EnactmentSpecification}.
     */
    public List<Cut> schedule(EnactmentSpecification specification) {

        ResourceGraph rGraph = specification.getResourceGraph();
        Collection<net.sf.opendse.model.Resource> rVertices = rGraph.getVertices();

        List<Resource> resources = new ArrayList<>();
        for(net.sf.opendse.model.Resource r: rVertices){
            resources.add(new Resource(r.getId(), PropertyServiceScheduler.getInstances(r),
                PropertyServiceScheduler.getLatencyLocal(r),
                PropertyServiceScheduler.getLatencyGlobal(r))
            );
        }

        EnactmentGraph eGraph = specification.getEnactmentGraph();
        MappingsConcurrent mappings = specification.getMappings();

        // Rank the tasks based on upward rank (no latency between tasks)

        List<Task> tasks = new ArrayList<>();
        tasks.addAll(specification.getEnactmentGraph().getVertices());
        ArrayList<Task> rankedTasks = rank2(
            tasks, specification);
        Stack rankedTaskStack = new Stack<net.sf.opendse.model.Task>();
        rankedTaskStack.addAll(rankedTasks);

        while(!rankedTaskStack.isEmpty()){

            net.sf.opendse.model.Task rankedTask = (Task) rankedTaskStack.pop();

            // Get predecessor task nodes of current ranked task
            Collection<Task> predecessorTaskNodes = getPredecessorTaskNodes(eGraph, rankedTask);

            // Earliest start time the function can start (maximum finish time of predecessor tasks)
            double earliestStartTime = 0;

            // Find the minimum start time by inspecting the finish times of the predecessor tasks
            for (Task p : predecessorTaskNodes) {
                double finishTime = mapFinishTime.get(p);
                if (earliestStartTime < finishTime) {
                    earliestStartTime = finishTime;
                }
            }


            // Variables for the best suited resource for the current task
            Resource bestResource = null;
            boolean bestPrevTaskOnSameResource = false;
            double bestEft = 0;
            double bestDuration = 0;

            // Earliest finish time
            double eft = Double.MAX_VALUE;

            // The mappings of the ranked task
            Set<Mapping<Task, net.sf.opendse.model.Resource>> mappingsRankedTask = mappings.getMappings(rankedTask);

            // Iterate over all available resources
            for(Resource resource: resources) {

                // Check if previous task was on the same resource
                boolean tmpPrevTaskOnSameResource = false;
                for(Task predecessor: predecessorTaskNodes){
                     if(mapResource.get(predecessor).getType().equals(resource.getType())) {
                        tmpPrevTaskOnSameResource = true;
                        break;
                     }
                }

                // Get the duration of the ranked task on resource r
                double duration = -1.0;
                for(Mapping<Task, net.sf.opendse.model.Resource> mR: mappingsRankedTask){
                    if(mR.getTarget().getId().contains(resource.getType())) {
                        duration = PropertyServiceScheduler.getDuration(mR);
                    }
                }
                if(duration == -1.0){
                    throw new IllegalArgumentException(
                        "Node " + rankedTask.getId() + " has no function duration on resource " + resource.getType());
                }


                // Calculate potential earliest finish time
                double tmpEft = resource.earliestStartTime(earliestStartTime, tmpPrevTaskOnSameResource) + duration;

                // Remember resource if it is better than the previous one
                if (eft > tmpEft) {
                    bestResource = resource;
                    bestPrevTaskOnSameResource = tmpPrevTaskOnSameResource;
                    bestEft = tmpEft;
                    bestDuration = duration;
                }
                eft = tmpEft;
            }

            // Set that the resource is used now
            assert bestResource != null;
            bestResource.setResource(earliestStartTime, bestDuration, bestPrevTaskOnSameResource);
            System.out.println(mapRank);
            mapResource.put(rankedTask, bestResource);

            // Set the finish time of the task and its resource type
            mapFinishTime.put(rankedTask, bestEft);

            // Keep only best mapping
            for(Mapping<Task, net.sf.opendse.model.Resource> mR: mappingsRankedTask){
                if(!mR.getTarget().getId().contains(bestResource.getType())){
                    mappings.removeMapping(mR);
                }
            }

            Stack rankedTaskStack2 = new Stack<net.sf.opendse.model.Task>();
            rankedTaskStack2.addAll(rank2(new ArrayList(rankedTaskStack), specification));

            rankedTaskStack.clear();
            rankedTaskStack.addAll(rankedTaskStack2);
        }

        return extractCuts(eGraph, rankedTasks);
    }
}
