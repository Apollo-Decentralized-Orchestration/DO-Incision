package at.uibk.dps.di.incision;

import at.uibk.dps.ee.model.graph.EnactmentGraph;
import at.uibk.dps.ee.model.properties.PropertyServiceData;
import at.uibk.dps.ee.model.properties.PropertyServiceDependency;
import at.uibk.dps.ee.model.utils.UtilsCopy;
import net.sf.opendse.model.Communication;
import net.sf.opendse.model.Dependency;
import net.sf.opendse.model.Task;
import net.sf.opendse.model.properties.TaskPropertyService;

import java.util.AbstractMap;
import java.util.HashSet;
import java.util.Set;
import java.util.Stack;
import java.util.stream.Collectors;

/**
 * Class to cut the {@link EnactmentGraph} at two given cuts.
 *
 * @author Stefan Pedratscher
 */
public class Incision {

  /**
   * Cut the {@link EnactmentGraph} at a specific position (two given cuts).
   *
   * @param eGraph the reference to the original input {@link EnactmentGraph}.
   *        This {@link EnactmentGraph} will be adapted and contains the
   *        distributed engine node after this method call.
   * @param topCut communication nodes representing the top cut of the
   *        {@link EnactmentGraph}.
   * @param bottomCut communication nodes representing the bottom cut of the
   *        {@link EnactmentGraph}.
   *
   * @return the resulting cut out {@link EnactmentGraph}.
   *
   * @throws IllegalArgumentException if the cut is invalid.
   */
  public EnactmentGraph cut(final EnactmentGraph eGraph, final Set<Task> topCut,
      final Set<Task> bottomCut) {

    validateInput(eGraph, topCut, bottomCut);

    // Create the cut out graph
    final EnactmentGraph cutOutGraph = cutGraph(eGraph, topCut, bottomCut);

    insertFunctionNode(eGraph, topCut, bottomCut);

    // Remove outsourced edges and vertices from the initial graph.
    cutOutGraph.getEdges().forEach((edge) -> eGraph.removeEdge(eGraph.getEdge(edge.getId())));
    cutOutGraph.getVertices().forEach((vertice) -> {
      if(!(topCut.contains(vertice) || bottomCut.contains(vertice))) {
        eGraph.removeVertex(eGraph.getVertex(vertice.getId()));
      }
    });

    // Mark leaf and root nodes of the cut out graph
    bottomCut.forEach((bTask) -> PropertyServiceData.makeLeaf(cutOutGraph.getVertex(bTask)));
    topCut.forEach((tTask) -> PropertyServiceData.makeRoot(cutOutGraph.getVertex(tTask)));

    return cutOutGraph;
  }

  /**
   * Insert a function node and remap dependencies.
   *
   * @param eGraph the reference to the original input {@link EnactmentGraph}.
   *         This {@link EnactmentGraph} will be adapted and contains
   *         the distributed engine node after this method call.
   * @param topCut communication nodes representing the top cut of the
   *        {@link EnactmentGraph}.
   * @param bottomCut communication nodes representing the bottom cut
   *        of the {@link EnactmentGraph}.
   */
  private void insertFunctionNode(final EnactmentGraph eGraph, final Set<Task> topCut, final Set<Task> bottomCut) {
    // Create and insert the function node for the distributed engine
    final Task functionNode = new Task(topCut.toString() + bottomCut.toString());
    eGraph.addVertex(functionNode);

    // Substitute the edges before the bottom cut
    for (final Task bTask : bottomCut) {
      eGraph.getInEdges(bTask).forEach(
          dependency -> remapDependency(eGraph, dependency, functionNode, bTask));
    }

    // Substitute the edges after the top cut
    for (final Task tTask : topCut) {
      eGraph.getOutEdges(tTask).forEach(
          dependency -> remapDependency(eGraph, dependency, tTask, functionNode));
    }
  }

  /**
   * Validate the input to the cutting method.
   *
   * @param eGraph the reference to the original input {@link EnactmentGraph}.
   *         This {@link EnactmentGraph} will be adapted and contains the
   *         distributed engine node after this method call.
   * @param topCut communication nodes representing the top cut of the
   *        {@link EnactmentGraph}.
   * @param bottomCut communication nodes representing the bottom cut of
   *        the {@link EnactmentGraph}.
   */
  private void validateInput(final EnactmentGraph eGraph, final Set<Task> topCut, final Set<Task> bottomCut){
    // Check if top and bottom cut is specified
    if (topCut == null || topCut.isEmpty() || bottomCut == null || bottomCut.isEmpty()) {
      throw new IllegalArgumentException("Both, top and bottom cut must be specified!");
    }

    // Check if elements in top and bottom cut are communication nodes
    topCut.forEach((task) -> {if(!(task instanceof Communication)) { throw new IllegalArgumentException("Top cut must be a set of communication nodes!");}} );
    bottomCut.forEach((task) -> {if(!(task instanceof Communication)) { throw new IllegalArgumentException("Bottom cut must be a set of communication nodes!");}} );

    // Check if the cut is valid
    if(!isCutValid(eGraph, topCut, bottomCut)) {
      throw new IllegalArgumentException("The cut is invalid!");
    }
  }

  /**
   * Check if the given cut is valid.
   *
   * @param eGraph the {@link EnactmentGraph} to check for a valid cut.
   * @param topCut the top cut communication nodes of the
   *               {@link EnactmentGraph}.
   * @param bottomCut the bottom cut communication nodes of the
   *                  {@link EnactmentGraph}.
   *
   * @return true if the cut is valid.
   */
  boolean isCutValid(final EnactmentGraph eGraph, final Set<Task> topCut, final Set<Task> bottomCut) {
    // Check both directions for validity
    return checkDirection(eGraph, topCut, bottomCut, true)
        && checkDirection(eGraph, bottomCut, topCut, false);
  }

  /**
   * Check a specific direction (topBottom or bottomTop) of the cut
   * for validity.
   *
   * @param eGraph {@link EnactmentGraph} to check for a valid cut.
   * @param startTasks represents the starting tasks to check for
   *                   validity.
   * @param endTasks represents the end tasks to stop checking for
   *                 validity.
   * @param topBottom specifies the direction to check: from top to
   *                  bottom or bottom to top.
   *
   * @return true if cut is valid for the specified direction.
   */
  private boolean checkDirection(final EnactmentGraph eGraph, final Set<Task> startTasks, final Set<Task> endTasks,
      final boolean topBottom) {
    Set<Task> currentTasks = new HashSet<>(startTasks);
    while (!currentTasks.isEmpty()) {

      // Get the next tasks
      final Set<Task> nextTasks = currentTasks.stream()
          .flatMap(curTask -> (topBottom ? eGraph.getSuccessors(curTask)
              : eGraph.getPredecessors(curTask)).stream())
          .filter(newTask -> !endTasks.contains(newTask))
          .filter(newTask -> !(TaskPropertyService.isCommunication(newTask)
              && PropertyServiceData.isConstantNode(newTask)))
          .collect(Collectors.toSet());

      // Check for node without next step
      if (nextTasks.stream()
          .anyMatch(nextTask -> topBottom ? eGraph.getSuccessorCount(nextTask) == 0
              : eGraph.getPredecessorCount(nextTask) == 0)) {
        return false;
      }
      currentTasks = nextTasks;
    }
    return true;
  }

  /**
   * Remaps the edges of the cut to the node representing the
   * distributed engine.
   *
   * @param eGraph the full {@link EnactmentGraph}.
   * @param dependency the dependency edge to remap.
   * @param taskFrom new source of the edge.
   * @param taskTo new destination of the edge.
   */
  private void remapDependency(final EnactmentGraph eGraph, final Dependency dependency,
      final Task taskFrom, final Task taskTo) {
    final Dependency tmp = UtilsCopy.deepCopyDependency(dependency);
    eGraph.removeEdge(dependency);
    PropertyServiceDependency.addDataDependency(taskFrom, taskTo, PropertyServiceDependency.getJsonKey(tmp), eGraph);
  }

  /**
   * Copy the tasks and edges below the current task.
   *
   * @param eGraph the full {@link EnactmentGraph}.
   * @param cutOutGraph the cut out {@link EnactmentGraph}
   *                    to add tasks and edges to.
   * @param currentTasks the current tasks to check.
   * @param task the current task.
   */
  private void copyBelow(final EnactmentGraph eGraph, final EnactmentGraph cutOutGraph,
      final Stack<AbstractMap.SimpleEntry<Task, Dependency>> currentTasks,
      final AbstractMap.SimpleEntry<Task, Dependency> task){

    for (final Dependency edge : eGraph.getOutEdges(task.getKey())) {
      if (task.getValue() == null || !task.getValue().equals(edge)) {
        copyEdge(cutOutGraph, edge, currentTasks, task.getKey(), eGraph.getDest(edge));
      }
    }
  }

  /**
   * Copy the tasks and edges above the current task.
   *
   * @param eGraph the full {@link EnactmentGraph}.
   * @param cutOutGraph the cut out {@link EnactmentGraph}
   *                    to add tasks and edges to.
   * @param currentTasks the current tasks to check.
   * @param task the current task.
   */
  private void copyAbove(final EnactmentGraph eGraph, final EnactmentGraph cutOutGraph,
      final Stack<AbstractMap.SimpleEntry<Task, Dependency>> currentTasks,
      final AbstractMap.SimpleEntry<Task, Dependency> task){

    for (final Dependency edge : eGraph.getInEdges(task.getKey())) {
      if (task.getValue() == null || !task.getValue().equals(edge)) {
        copyEdge(cutOutGraph, edge, currentTasks, task.getKey(), eGraph.getSource(edge));
      }
    }
  }

  /**
   * Copy the edge with the corresponding source and
   * destination tasks.
   *
   * @param cutOutGraph the cut out {@link EnactmentGraph}.
   * @param edge the edge to copy.
   * @param currentTasks the list of current tasks.
   * @param currentTask the current task to copy.
   * @param nextTask the next task to copy
   */
  private void copyEdge(final EnactmentGraph cutOutGraph, final Dependency edge,
      final Stack<AbstractMap.SimpleEntry<Task, Dependency>> currentTasks,
      final Task currentTask, final Task nextTask) {

    // Copy the current and next task
    final Task taskFrom = currentTask instanceof Communication ? UtilsCopy.deepCopyCommunication(currentTask)
        : UtilsCopy.deepCopyTask(currentTask);
    final Task taskTo = nextTask instanceof Communication ? UtilsCopy.deepCopyCommunication(nextTask)
        : UtilsCopy.deepCopyTask(nextTask);

    // Make a copy of the edge
    final Dependency tmpEdge = UtilsCopy.deepCopyDependency(edge);

    // Add edge with the given tasks to the cut out graph
    PropertyServiceDependency.addDataDependency(taskFrom, taskTo, PropertyServiceDependency.getJsonKey(tmpEdge), cutOutGraph);

    // Add next task to check
    currentTasks.push(new AbstractMap.SimpleEntry<>(nextTask, tmpEdge));
  }

  /**
   * Cut the {@link EnactmentGraph}.
   *
   * @param eGraph the {@link EnactmentGraph} to cut.
   * @param startCut the start nodes representing the cut.
   * @param endCut the end node representing the cut.
   */
  private EnactmentGraph cutGraph(final EnactmentGraph eGraph, final Set<Task> startCut, final Set<Task> endCut) {
    // Begin with the start tasks
    final Stack<AbstractMap.SimpleEntry<Task, Dependency>> currentTasks = new Stack<>();
    startCut.forEach(node -> currentTasks.push(new AbstractMap.SimpleEntry<>(node, null)));

    final EnactmentGraph cutOutGraph = new EnactmentGraph();

    // Continue if there are more tasks to proceed
    while(!currentTasks.isEmpty()) {

      // Get one task of the stack
      final AbstractMap.SimpleEntry<Task, Dependency> currentTask = currentTasks.pop();

      // Check if task is in end or start tasks
      final boolean isEndTask = endCut.contains(currentTask.getKey());
      final boolean isStartTask = startCut.contains(currentTask.getKey());

      if(isEndTask || !isStartTask) {

        // Continue with the tasks above
        copyAbove(eGraph, cutOutGraph, currentTasks, currentTask);
      }
      if(isStartTask || !isEndTask) {

        // Continue with the tasks below
        copyBelow(eGraph, cutOutGraph, currentTasks, currentTask);
      }
    }

    return cutOutGraph;
  }
}
