package at.uibk.dps.di.scheduler;

import at.uibk.dps.di.incision.Utility;
import at.uibk.dps.di.properties.PropertyServiceScheduler;
import at.uibk.dps.ee.model.graph.EnactmentGraph;
import at.uibk.dps.ee.model.graph.EnactmentSpecification;
import at.uibk.dps.ee.model.graph.MappingsConcurrent;
import at.uibk.dps.ee.model.graph.ResourceGraph;
import at.uibk.dps.ee.model.properties.PropertyServiceData;
import net.sf.opendse.model.Communication;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Task;
import org.apache.commons.lang.StringUtils;

import java.util.*;
import java.util.stream.Collectors;

public class Scheduler {

    boolean dynRank = true;
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
    private final Map<Task, Resource> mapResource;

    /**
     * Default constructor
     */
    public Scheduler(){
        mapRank = new HashMap<>();
        mapFinishTime = new HashMap<>();
        mapResource = new HashMap<>();
    }

    /**
     * Get the leaf nodes of an enactment graph.
     *
     * @param eGraph the graph to check for leaf nodes.
     *
     * @return the leaf nodes.
     */
    private Collection<Task> getLeafNodes(EnactmentGraph eGraph) {
        return eGraph.getVertices()
                .stream()
                .filter(task -> task instanceof Communication && PropertyServiceData.isLeaf(task))
                .collect(Collectors.toList());
    }

    private Collection<Task> getRootNodes(EnactmentGraph eGraph) {
        return eGraph.getVertices()
                .stream()
                .filter(task -> task instanceof Communication && PropertyServiceData.isRoot(task))
                .collect(Collectors.toList());
    }

    /**
     * Get all immediate successor task nodes of a specific task.
     *
     * @param eGraph the graph to look for successors.
     * @param node the task node to check for immediate task successors.
     *
     * @return the immediate successor task nodes.
     */
    private Collection<Task> getSuccessorTaskNodes(EnactmentGraph eGraph, Task node) {
        if (node instanceof Communication) {

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
     * Get all immediate predecessor task nodes of a specific task.
     *
     * @param eGraph the graph to look for predecessors.
     * @param node the task node to check for immediate task predecessors.
     *
     *  @return the immediate predecessor task nodes.
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
     * Calculate the rank of a given task.
     *
     * @param currentTaskRank the rank of the current task.
     * @param predecessor the predecessor task for which the rank should be calculated.
     * @param mappings the s mappings.
     * @param rankedTasks resulting list of ranked tasks.
     * @param toConsider contains all tasks that should be ranked.
     *
     * @return the rank of the predecessor.
     */
    private double calcRank(double currentTaskRank, Task predecessor, MappingsConcurrent mappings,
                             ArrayList<Task> rankedTasks, List<Task> toConsider, boolean override){

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
            if(override && toConsider.contains(predecessor)) {
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
     * Rank all tasks specified in a list.
     *
     * @param tasks the tasks to rank.
     * @param specification the enactment specification.
     *
     * @return a list of ranked tasks.
     */
    public ArrayList<Task> rank(List<Task> tasks, EnactmentSpecification specification) {

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

                double currentTaskRank = calcRank(current.getValue(), predecessor, mappings, rankedTasks, tasks, false);

                // Add predecessor and its rank to the stack
                nodeStack.add(new AbstractMap.SimpleEntry<>(predecessor, currentTaskRank));
                //predecessor.setAttribute("rank", currentTaskRank);
            }
        }

        return rankedTasks;
    }

    public ArrayList<Task> rankDownWards(List<Task> tasks, EnactmentSpecification specification, boolean override) {

        EnactmentGraph eGraph = specification.getEnactmentGraph();
        MappingsConcurrent mappings = specification.getMappings();

        ArrayList<Task> rankedTasks = new ArrayList<>();

        // Stack containing nodes to check
        Stack<AbstractMap.SimpleEntry<Task, Double>> nodeStack = new Stack<>();
        getRootNodes(eGraph).forEach(node -> nodeStack.push(new AbstractMap.SimpleEntry<>(node, 0.0)));

        // Continue until all tasks are ranked
        while (!nodeStack.isEmpty()) {

            AbstractMap.SimpleEntry<Task, Double> current = nodeStack.pop();


            // Get predecessors of the current task on the stack
            Collection<Task> successorNodes = eGraph.getSuccessors(current.getKey());

            // Iterate over all successor nodes
            for (Task successor : successorNodes) {

                double currentTaskRank = calcRank(current.getValue(), successor, mappings, rankedTasks, tasks, override);

                // Add successor and its rank to the stack
                nodeStack.add(new AbstractMap.SimpleEntry<>(successor, currentTaskRank));
                //successor.setAttribute("rank", currentTaskRank);
            }

        }

        return rankedTasks;
    }


    /**
     * Sort the tasks based on their rank.
     *
     * @param rankedTasks the tasks to sort.
     *
     * @return the sorted tasks.
     */
    public ArrayList<Task> sort(ArrayList<Task> rankedTasks){

        // Sort the given ranks
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

    public ArrayList<Task> sortOther(ArrayList<Task> rankedTasks){

        // Sort the given ranks
        rankedTasks.sort((o1, o2) -> {
            double rank1 = mapRank.get(o1);
            double rank2 = mapRank.get(o2);
            if (rank1 == rank2) {
                return 0;
            }
            return rank1 > rank2 ? -1 : 1;
        });

        return rankedTasks;
    }

    /**
     * Get a specific resource.
     *
     * @param resources list of resources to check.
     * @param typeToLookFor the type to check.
     *
     * @return the desired resource.
     */
    public Resource getResource(List<Resource> resources, String typeToLookFor) {

        // Iterate over all resources
        for(Resource r: resources) {

            // Check if it is the desired resource
            if(r.getType().equals(typeToLookFor)){
                return r;
            }
        }
        throw new IllegalArgumentException("Could not find resource type " + typeToLookFor);
    }

    /**
     * Generate the cuts out of resource assignment.
     *
     * @param eGraph the enactment graph.
     * @param rankedTasks the ranked tasks with resources.
     *
     *  @return the generated cuts.
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
                boolean singleTaskInCut = true;
                Set<Task> topCut = new HashSet<>();
                Set<Task> bottomCut = new HashSet<>();

                // Create stack for the predecessor task nodes
                Stack<Task> stackPredecessor = new Stack<>();
                stackPredecessor.addAll(getPredecessorTaskNodes(eGraph, current));
                Task prev = current;

                // While there are predecessors check if they are on the same resource
                while (!stackPredecessor.isEmpty()) {
                    Task pre = stackPredecessor.pop();

                    // Check if on the same resource
                    if (mapResource.get(pre).getType().equals(currentResource.getType())) {

                        singleTaskInCut = false;

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

                // While there are successors check if they are on the same resource
                while (!stackSuccessors.isEmpty()) {
                    Task suc = stackSuccessors.pop();

                    // Check if on the same resource
                    if (mapResource.get(suc).getType().equals(currentResource.getType())) {

                        singleTaskInCut = false;

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
                if(!singleTaskInCut) {
                    proposedCuts.add(new Cut(topCut, bottomCut));
                }
            }
        }
        return proposedCuts;
    }

    /**
     * Get all successor ranked tasks starting from a specific task and
     * the next ranked highest task, if present.
     *
     * @param rankedTaskStack the ranked tasks to check.
     * @param first the task to start looking for successor.
     * @param eGraph the enactment graph to get successors from.
     *
     * @return the successor ranked tasks.
     */
    private ArrayList<Task> getSuccessorRankedTasks(Stack<Task> rankedTaskStack, Task first, EnactmentGraph eGraph){

        // Create a stack for the task to check for successors
        Stack<Task> tasksToAdd = new Stack<>();

        // Add all successors of initial task to the stack
        tasksToAdd.addAll(getSuccessorTaskNodes(eGraph, first));

        // If possible consider also the next ranked task
        if(rankedTaskStack.size() > 1) {
            tasksToAdd.add(rankedTaskStack.pop());
        }

        // Create list for resulting ranked tasks
        ArrayList<Task> result = new ArrayList<>();

        // While there are tasks to check
        while (!tasksToAdd.isEmpty()) {

            // Take a task from the list and add its successors to the list
            Task task = tasksToAdd.pop();
            tasksToAdd.addAll(getSuccessorTaskNodes(eGraph, task));

            // Add it to the resulting list if not already added
            if(!result.contains(task)){
                result.add(task);
            }
        }
        return result;
    }

    /**
     * Get the duration of a task.
     *
     * @param rankedTask the task to get the duration for.
     * @param mappingsRankedTask the mappings of the ranked task.
     * @param resource the resource of the task to get the duration for.
     *
     * @return the duration of the task on the specified resource.
     */
    private double getDuration(Task rankedTask, Set<Mapping<Task, net.sf.opendse.model.Resource>> mappingsRankedTask, Resource resource) {
        for(Mapping<Task, net.sf.opendse.model.Resource> mR: mappingsRankedTask){
            if(mR.getTarget().getId().contains(resource.getType())) {
                return PropertyServiceScheduler.getDuration(mR);
            }
        }
        throw new IllegalArgumentException(
                "Node " + rankedTask.getId() + " has no function duration on resource " + resource.getType());
    }

    /**
     * Perform HEFT on the given input.
     *
     * @param currentTask the task to start with heft.
     * @param rankedTasks all ranked tasks that should be scheduled in heft.
     * @param resourceOfCurrentTask the resource of the task to start with.
     * @param eftOfCurrentTask the earliest finish time of the task to start with.
     * @param specification the full enactment specification.
     * @param resources the resources on which the tasks can run on.
     * @param earliestStartTimeOfCurrentTask the earliest start time og the task to start with.
     * @param durationCurrentTask the duration of the task to start with.
     * @param prevTaskOnSameResourceCurrentTask determines if a previous task was on the same resource.
     *
     * @return the duration of the workflow after scheduled with HEFT.
     */
    public double heft(Task currentTask, ArrayList<Task> rankedTasks, Resource resourceOfCurrentTask,
                       double eftOfCurrentTask, EnactmentSpecification specification, List<Resource> resources,
                       double earliestStartTimeOfCurrentTask, double durationCurrentTask, boolean prevTaskOnSameResourceCurrentTask, int rec,
                       Map<Task, Double> mapFinishTimeGiven) {
        // Get specification, mappings and resource
        EnactmentGraph eGraph = specification.getEnactmentGraph();
        MappingsConcurrent mappings = specification.getMappings();
        Resource resource2 = getResource(resources, resourceOfCurrentTask.getType());

        // Create temporary hashmaps for finish time and resource assignment
        Map<Task, Double> mapFinishTimeTmp = new HashMap<>(mapFinishTimeGiven);
        Map<Task, Resource> mapResourceTmp = new HashMap<>(mapResource);

        //System.out.println(mapFinishTimeTmp);

        if(currentTask.getId().contains("5")){
            System.out.println();
        }
        // Add current task resource and finish time
        mapResourceTmp.put(currentTask, resource2);
        mapFinishTimeTmp.put(currentTask, eftOfCurrentTask);
        resource2.setResource(earliestStartTimeOfCurrentTask, durationCurrentTask, prevTaskOnSameResourceCurrentTask);

        // Create stack for ranked tasks
        Stack<Task> rankedTaskStack = new Stack<>();
        rankedTaskStack.addAll(rankedTasks);

        System.out.println("Spaces");
        for(Resource r: resources) {
            r.printSPaces();
        }

        // Continue if there are ranked tasks
        while(!rankedTaskStack.isEmpty()) {

            // Get task from stack
            net.sf.opendse.model.Task rankedTask = rankedTaskStack.pop();

            // Get predecessor task nodes of current ranked task
            Collection<Task> predecessorTaskNodes = getPredecessorTaskNodes(eGraph, rankedTask);


            if(rankedTask.getId().contains("7")){
                System.out.println();
            }

            // Earliest start time the function can start (maximum finish time of predecessor tasks)
            double earliestStartTime = 0;

            // Find the minimum start time by inspecting the finish times of the predecessor tasks
            for (Task p : predecessorTaskNodes) {
                if(mapFinishTimeTmp.containsKey(p)){
                    double finishTime = mapFinishTimeTmp.get(p);
                    if (earliestStartTime < finishTime) {
                        earliestStartTime = finishTime;
                    }
                }
            }

            // Variables for the best suited resource for the current task
            Resource bestResource = null;
            boolean bestPrevTaskOnSameResource = false;
            boolean bestPrevPrevTaskOnSameResource = false;
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
                boolean tmpPrevPrevTaskOnSameResource = false;
                for(Task predecessor: predecessorTaskNodes){
                    if(mapResourceTmp.containsKey(predecessor)) {
                        if (mapResourceTmp.get(predecessor).getType().equals(resource.getType())) {
                            tmpPrevTaskOnSameResource = true;
                            Collection<Task> predecessorPredTaskNodes = getPredecessorTaskNodes(eGraph, predecessor);
                            for(Task prepredecessor: predecessorPredTaskNodes){
                                if(mapResourceTmp.containsKey(prepredecessor) && mapResourceTmp.get(prepredecessor).getType().equals(resource.getType())) {
                                    tmpPrevPrevTaskOnSameResource = true;
                                }
                            }
                            break;
                        }
                    }
                }

                double ll = resource.getLatencyLocal();

                if(!resource.getType().contains("Local")) {
                    if (!tmpPrevTaskOnSameResource) {
                        resource.setLatencyLocal(0);
                    }
                    if (tmpPrevTaskOnSameResource && !tmpPrevPrevTaskOnSameResource) {
                        resource.setLatencyLocal(ll * 2);
                    }
                }

                // Get the duration of the ranked task on resource r
                double duration = getDuration(rankedTask, mappingsRankedTask, resource);

                double rankTMP = earliestStartTime + duration + resource.getLatencyLocal();
                if(!tmpPrevTaskOnSameResource) {
                    rankTMP += resource.getLatencyGlobal();
                } /*else {
                    rankTMP += resource.getLatencyLocal();
                    if(!tmpPrevPrevTaskOnSameResource) {
                        rankTMP += resource.getLatencyLocal();
                    }
                }*/
                rankTMP = /*mapRank.get(rankedTask) - */ -rankTMP;
                Double tmp = mapRank.remove(rankedTask);
                mapRank.put(rankedTask, rankTMP);

                if(dynRank) {
                    // Dynamically rerank
                    rankedTasks = sortOther(rankDownWards(rankedTaskStack, specification, true));
                    rankedTaskStack = new Stack<>();
                    rankedTaskStack.addAll(rankedTasks);
                }
                //System.out.println(mapRank);

                double tmpEft = -1;
                rec = 0;
                if(rec == 0) {
                    // Calculate potential earliest finish time
                    System.out.print(rankedTask.getId());
                    tmpEft = resource.earliestStartTime(earliestStartTime, tmpPrevTaskOnSameResource, duration);
                    if (eft > tmpEft) {
                        bestResource = resource;
                        bestPrevTaskOnSameResource = tmpPrevTaskOnSameResource;
                        bestPrevPrevTaskOnSameResource = tmpPrevPrevTaskOnSameResource;
                        bestEft = tmpEft;
                        bestDuration = duration;
                    }
                    eft = tmpEft;
                } else {
                    ArrayList<Task> recallRankedTasks = sortOther(getSuccessorRankedTasks((Stack<Task>) rankedTaskStack.clone(), rankedTask, eGraph));
                    double recallEst = resource.earliestStartTime(earliestStartTime, tmpPrevTaskOnSameResource, duration);
                    ArrayList<Resource> recallResources = new ArrayList<>();
                    for(Resource res: resources){
                        Resource recallRes = new Resource(res.getType(), res.getTotalNumInstances(), res.getLatencyLocal(), res.getLatencyGlobal());
                        List<Double> avail = new ArrayList<>(res.getAvailable());
                        recallRes.setAvailable(avail);
                        recallResources.add(recallRes);
                    }
                    if(rankedTask.getId().contains("7")){
                        System.out.println();
                    }

                    tmpEft = heft(rankedTask, recallRankedTasks, recallResources.get(0), recallEst, specification, recallResources, earliestStartTime, duration, tmpPrevTaskOnSameResource, rec, mapFinishTimeTmp);

                    double tmpEft2 = heft(rankedTask, recallRankedTasks, recallResources.get(1), recallEst, specification, recallResources, earliestStartTime, duration, tmpPrevTaskOnSameResource, rec, mapFinishTimeTmp);
                    rec--;
                    if(tmpEft < tmpEft2) {
                            bestResource = recallResources.get(0);
                            bestPrevTaskOnSameResource = tmpPrevTaskOnSameResource;
                            bestPrevPrevTaskOnSameResource = tmpPrevPrevTaskOnSameResource;
                            bestEft = tmpEft;
                            bestDuration = duration;
                        eft = tmpEft;
                    }else {
                            bestResource = recallResources.get(1);
                            bestPrevTaskOnSameResource = tmpPrevTaskOnSameResource;
                            bestPrevPrevTaskOnSameResource = tmpPrevPrevTaskOnSameResource;
                            bestEft = tmpEft2;
                            bestDuration = duration;
                        eft = tmpEft2;
                    }
                }

                // Remember resource if it is better than the previous one
                /*if (eft > tmpEft) {
                    bestResource = resource;
                    bestPrevTaskOnSameResource = tmpPrevTaskOnSameResource;
                    bestPrevPrevTaskOnSameResource = tmpPrevPrevTaskOnSameResource;
                    bestEft = tmpEft;
                    bestDuration = duration;
                }
                eft = tmpEft;
                */

                mapRank.remove(rankedTask);
                mapRank.put(rankedTask, tmp);
                resource.setLatencyLocal(ll);
            }
            double rankTMP = earliestStartTime + bestDuration + bestResource.getLatencyLocal();
            if(!bestPrevTaskOnSameResource) {
                rankTMP += bestResource.getLatencyGlobal();
            }
            rankTMP = /*mapRank.get(rankedTask) - */ -rankTMP;
            mapRank.remove(rankedTask);
            mapRank.put(rankedTask, rankTMP);
            if(dynRank) {
                // Dynamically rerank
                rankedTasks = sortOther(rankDownWards(rankedTaskStack, specification, false));
                rankedTaskStack = new Stack<>();
                rankedTaskStack.addAll(rankedTasks);
            }
            // Set that the resource is used now
            assert bestResource != null;

            double ll = bestResource.getLatencyLocal();
            if(!bestResource.getType().contains("Local")) {
                if(!bestPrevTaskOnSameResource) {
                    bestResource.setLatencyLocal(0);
                }
                if(bestPrevTaskOnSameResource && !bestPrevPrevTaskOnSameResource) {
                    bestResource.setLatencyLocal(ll * 2);
                }
            }

            Double actualFt = bestResource.setResource(earliestStartTime, bestDuration, bestPrevTaskOnSameResource);
            mapResourceTmp.put(rankedTask, bestResource);
            rankedTask.setAttribute("tmpHeft-" + currentTask.getId() + "-r-" + resourceOfCurrentTask.getType(), bestResource.getType().contains("Local") ? "L" : "Cloud");
            //System.out.println("-----" + currentTask.getId() + " on " + resourceOfCurrentTask.getType() + ": best " + bestResource.getType() + " for " + rankedTask.getId() + ", bc est=" +earliestStartTime + " and eft=" + bestEft + ", actual FT: " + actualFt);
            //rankedTask.setAttribute("rankHeft" + runn, mapRank.get(rankedTask));

            // Set the finish time of the task and its resource type
            mapFinishTimeTmp.put(rankedTask, bestEft);

            bestResource.setLatencyLocal(ll);
        }
        runn++;
        Set<Double> resourceDurations = mapResourceTmp.values().stream()
                .map(at.uibk.dps.di.scheduler.Resource::maxDuration)
                .collect(Collectors.toSet());

        return resourceDurations.isEmpty() ? 0.0 : Collections.max(resourceDurations);
    }

    int runn = 0;

    /**
     * Evaluate and schedule the tasks to the resources.
     *
     * @param specification the {@link EnactmentSpecification}.
     */
    public List<Cut> schedule(EnactmentSpecification specification) {

        // Get the resource graph and the vertices of the specification
        ResourceGraph rGraph = specification.getResourceGraph();
        Collection<net.sf.opendse.model.Resource> rVertices = rGraph.getVertices();

        // Transform to an internal representation for the resources
        List<Resource> resources = new ArrayList<>();
        for(net.sf.opendse.model.Resource r: rVertices){
            resources.add(new Resource(r.getId(), PropertyServiceScheduler.getInstances(r),
                    PropertyServiceScheduler.getLatencyLocal(r),
                    PropertyServiceScheduler.getLatencyGlobal(r))
            );
        }

        // Get the enactment graph and the mappings from the specification
        EnactmentGraph eGraph = specification.getEnactmentGraph();
        MappingsConcurrent mappings = specification.getMappings();

        // Rank the tasks based on upward rank (no latency between tasks)
        List<Task> tasks = new ArrayList<>(eGraph.getVertices());
        //ArrayList<Task> rankedTasks = sort(rank(tasks, specification));
        ArrayList<Task> rankedTasks = sortOther(rankDownWards(tasks, specification, false));
        System.out.println(mapRank);
        Stack<Task> rankedTaskStack = new Stack<>();
        rankedTaskStack.addAll(rankedTasks);

        //System.out.println(mapRank);

        // Continue while there are ranked tasks
        while(!rankedTaskStack.isEmpty()) {

            // Get the task with highest rank
            Task rankedTask = rankedTaskStack.pop();

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
            boolean bestPrevPrevTaskOnSameResource = false;
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
                boolean tmpPrevPrevTaskOnSameResource = false;
                for(Task predecessor: predecessorTaskNodes){
                    if(mapResource.get(predecessor).getType().equals(resource.getType())) {
                        tmpPrevTaskOnSameResource = true;
                        Collection<Task> predecessorPredTaskNodes = getPredecessorTaskNodes(eGraph, predecessor);
                        for(Task prepredecessor: predecessorPredTaskNodes){
                            if(mapResource.get(prepredecessor).getType().equals(resource.getType())) {
                                tmpPrevPrevTaskOnSameResource = true;
                            }
                        }
                        break;
                    }
                }

                double ll = resource.getLatencyLocal();
                if(!resource.getType().contains("Local")) {
                    if (!tmpPrevTaskOnSameResource) {
                        resource.setLatencyLocal(0);
                    }
                    if (tmpPrevTaskOnSameResource && !tmpPrevPrevTaskOnSameResource) {
                        resource.setLatencyLocal(ll * 2);
                    }
                }

                // Get the duration of the ranked task on resource r
                double duration = getDuration(rankedTask, mappingsRankedTask, resource);

                double rankTMP = earliestStartTime + duration + resource.getLatencyLocal();
                if(!tmpPrevTaskOnSameResource) {
                    rankTMP += resource.getLatencyGlobal();
                } /*else {
                    rankTMP += resource.getLatencyLocal();
                    if(!tmpPrevPrevTaskOnSameResource) {
                        rankTMP += resource.getLatencyLocal();
                    }
                }*/
                rankTMP = /*mapRank.get(rankedTask) - */ -rankTMP;
                mapRank.remove(rankedTask);
                mapRank.put(rankedTask, rankTMP);

                if(dynRank) {
                    // Dynamically rerank
                    rankedTasks = sortOther(rankDownWards(rankedTaskStack, specification, true));
                    rankedTaskStack = new Stack<>();
                    rankedTaskStack.addAll(rankedTasks);
                }

                // --> START call the heft algorithm on a specific part of the workflow
                // Calculate the new ranked tasks that should be checked with heft
                ArrayList<Task> recallRankedTasks = sortOther(getSuccessorRankedTasks((Stack<Task>) rankedTaskStack.clone(), rankedTask, eGraph));

                // Calculate the earliest start time
                System.out.print(rankedTask);
                System.out.println(mapRank);
                double recallEst = resource.earliestStartTime(earliestStartTime, tmpPrevTaskOnSameResource, duration);


                resource.setLatencyLocal(ll);
                // Create a copy of the current resource state
                ArrayList<Resource> recallResources = new ArrayList<>();
                for(Resource res: resources){
                    Resource recallRes = new Resource(res.getType(), res.getTotalNumInstances(), res.getLatencyLocal(), res.getLatencyGlobal());
                    List<Double> avail = new ArrayList<>(res.getAvailable());
                    recallRes.setAvailable(avail);
                    recallRes.spaces = new ArrayList<>(res.spaces);
                    recallResources.add(recallRes);
                }


                /*for (Map.Entry<Task, Double> entry : mapRank.entrySet())
                    System.out.print(entry.getKey().getId() +"=" + entry.getValue() + ", ");
                System.out.println();
                rankedTasks.forEach((r) -> System.out.print(r.getId() + ","));
                System.out.println();
*/
                if(rankedTask.getId().contains("4")){
                    System.out.println();
                }

                // Schedule with heft
                System.out.println("\n\t\tChecking task: " + rankedTask.getId() + " on " + resource.getType());
                double tmpEft = heft(rankedTask, recallRankedTasks, resource, recallEst, specification, recallResources, earliestStartTime, duration, tmpPrevTaskOnSameResource, 1, mapFinishTime);
                System.out.println("EFT: " + tmpEft);
                // <-- END call the heft algorithm on a specific part of the workflow
                if(rankedTask.getId().contains("4")){
                    System.out.println();
                }
                // Remember resource if it is better than the previous one
                if (eft > tmpEft) {
                    bestResource = resource;
                    bestPrevTaskOnSameResource = tmpPrevTaskOnSameResource;
                    bestPrevPrevTaskOnSameResource = tmpPrevPrevTaskOnSameResource;
                    bestEft = recallEst;
                    bestDuration = duration;
                } else if(eft == tmpEft && !tmpPrevTaskOnSameResource){
                    bestResource = resource;
                    bestPrevTaskOnSameResource = tmpPrevTaskOnSameResource;
                    bestPrevPrevTaskOnSameResource = tmpPrevPrevTaskOnSameResource;
                    bestEft = recallEst;
                    bestDuration = duration;
                }
                eft = tmpEft;
            }

            // Set that the resource is used now
            assert bestResource != null;
            double ll = bestResource.getLatencyLocal();

            if(!bestResource.getType().contains("Local")) {
                if (!bestPrevTaskOnSameResource) {
                    bestResource.setLatencyLocal(0);
                }
                if (bestPrevTaskOnSameResource && !bestPrevPrevTaskOnSameResource) {
                    bestResource.setLatencyLocal(ll * 2);
                }
            }
            Double fTime = bestResource.setResource(earliestStartTime, bestDuration, bestPrevTaskOnSameResource);
            mapResource.put(rankedTask, bestResource);
            rankedTask.setAttribute("resource", bestResource.getType().contains("http") ? "Cloud" : "L");
            /*rankedTask.setAttribute("est", earliestStartTime);
            rankedTask.setAttribute("duration", bestDuration);
            rankedTask.setAttribute("preOnSame", bestPrevTaskOnSameResource);
*/

            int idx = 0;
            for(int i = 0; i < bestResource.getAvailable().size(); i++) {
                if(bestResource.getAvailable().get(i).equals(fTime)) {
                    idx = i;
                }
            }
            if(bestResource.view.size() < idx + 1) {
                bestResource.view.add("");
            }
            //System.out.println(rankedTask.getId() + " est" + earliestStartTime + " - " + bestResource.view.length());
            if(earliestStartTime/100 > bestResource.view.get(idx).length()) {
                bestResource.view.set(idx, bestResource.view.get(idx) + StringUtils.repeat(" ", (int) (earliestStartTime/100 - bestResource.view.get(idx).length())));
            }
            int durationActuial = (int) (fTime - earliestStartTime);
            bestResource.view.set(idx, bestResource.view.get(idx) + "[t" + rankedTask.getId().substring(rankedTask.getId().length()-2, rankedTask.getId().length()) + StringUtils.repeat("*", durationActuial / 100 - 5) + "]");

            //System.out.println("Task " + rankedTask.getId() + " on " + bestResource.getType() + " @ " + earliestStartTime + " until " + fTime +". Dur is " + (fTime - earliestStartTime));

            //System.out.println(rankedTask.getId() + " on " + bestResource.getType() + " - Ftime" + fTime + ". From " + earliestStartTime + " - " + bestResource.getAvailable() + "::" + StringUtils.repeat("7", (int) (Double.parseDouble(bestResource.getAvailable().get(0).toString())/10)));

            double rankTMP = earliestStartTime + bestDuration + bestResource.getLatencyLocal();
            if(!bestPrevTaskOnSameResource) {
                rankTMP += bestResource.getLatencyGlobal();
            }
            rankTMP = /*mapRank.get(rankedTask) - */ -rankTMP;
            mapRank.remove(rankedTask);
            mapRank.put(rankedTask, rankTMP);
            System.out.println("Rank" + rankTMP);
            if(dynRank) {
                rankedTasks = sortOther(rankDownWards(rankedTaskStack, specification, false));
                rankedTaskStack = new Stack<>();
                rankedTaskStack.addAll(rankedTasks);
            }
            // Set the finish time of the task and its resource type
            mapFinishTime.put(rankedTask, bestEft);
            //rankedTask.setAttribute("ft", bestEft);

            String some = bestResource.getType().contains("http") ? "Cloud" : "L";
            some += "; est: " + earliestStartTime + "; ft: " + bestEft + "; id: " + rankedTask.getId();
            //rankedTask.setAttribute("some", some);

            // Keep only best mapping
            for(Mapping<Task, net.sf.opendse.model.Resource> mR: mappingsRankedTask){
                if(!mR.getTarget().getId().contains(bestResource.getType())){
                    mappings.removeMapping(mR);
                }
            }

            bestResource.setLatencyLocal(ll);
        }


        System.out.print("T: ");
        for(int i = 1; i < 41; i++) {
            System.out.print(StringUtils.repeat(" ", 4) + (i));
        }
        System.out.println();

        for(Resource r: resources) {
            String pref = r.getType().contains("Local") ? "L" : "C";
            for(String s: r.view){
                System.out.println(pref + ": " + s);
            }
        }

        System.out.println("Spaces");
        for(Resource r: resources) {
            r.printSPaces();
        }


        // Extract the cuts from the new resource mappings
        return extractCuts(eGraph, sortOther(rankDownWards(new ArrayList<>(eGraph.getVertices()), specification, false)));
    }
}
