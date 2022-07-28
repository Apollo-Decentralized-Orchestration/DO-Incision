package at.uibk.dps.di.schedulerV2;

import at.uibk.dps.di.properties.PropertyServiceScheduler;
import at.uibk.dps.ee.model.graph.EnactmentGraph;
import at.uibk.dps.ee.model.graph.EnactmentSpecification;
import at.uibk.dps.ee.model.graph.MappingsConcurrent;
import at.uibk.dps.ee.model.properties.PropertyServiceData;
import net.sf.opendse.model.Communication;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

public class GraphUtility {

    public static int counter = 0;

    public static Collection<Task> getLeafNodes(EnactmentGraph eGraph) {
        return eGraph.getVertices()
            .stream()
            .filter(task -> task instanceof Communication && PropertyServiceData.isLeaf(task))
            .collect(Collectors.toList());
    }

    public static Collection<Task> getRootNodes(EnactmentGraph eGraph) {
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
    public static Collection<Task> getSuccessorTaskNodes(EnactmentGraph eGraph, Task node) {
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
    public static Collection<Task> getPredecessorTaskNodes(EnactmentGraph eGraph, Task node) {
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

    public static double getAvgDurationOnAllResources(EnactmentSpecification specification, Task task) {
        Set<Mapping<Task, Resource>> predecessorMappings = specification.getMappings().getMappings(task);

        // Check if duration attribute is specified
        if (predecessorMappings.isEmpty()) {
            throw new IllegalArgumentException(
                "Node " + task.getId() + " has no function duration");
        }

        // Get the average duration of the predecessor task node
        double duration = predecessorMappings.stream().mapToDouble(PropertyServiceScheduler::getDuration).sum() / predecessorMappings.size();

        return duration;
    }

    public static double getTaskDurationOnResource(EnactmentSpecification specification, Task task, ResourceV2 resource) {
        Set<Mapping<Task, Resource>> predecessorMappings = specification.getMappings().getMappings(task);

        // Get the average duration of the predecessor task node
        for (Mapping<Task, Resource> predecessorMapping : predecessorMappings) {
            if(predecessorMapping.getTarget().getId().equals(resource.getId())){
                return PropertyServiceScheduler.getDuration(predecessorMapping);
            }
        }

        throw new IllegalArgumentException("Could not find duration for " + task.getId() + " on resource " + resource.getId());
    }
}
