package at.uibk.dps.di.incision;

import at.uibk.dps.ee.model.graph.*;
import at.uibk.dps.ee.model.properties.*;
import at.uibk.dps.ee.model.utils.UtilsCopy;
import com.google.gson.JsonPrimitive;
import edu.uci.ics.jung.graph.util.EdgeType;
import net.sf.opendse.model.*;
import net.sf.opendse.model.properties.TaskPropertyService;

import java.util.*;
import java.util.stream.Collectors;

/**
 * Class to cut the {@link EnactmentGraph} at two given cuts.
 *
 * @author Stefan Pedratscher
 */
public class Incision {

  /**
   * Cut the {@link EnactmentGraph} at a specific position (two given cuts)
   * and adapt the {@link EnactmentSpecification}.
   *
   * @param enactmentSpecification the reference to the original
   *        input {@link EnactmentSpecification}. This
   *        {@link EnactmentSpecification} will be adapted and
   *        contains the distributed engine after this method
   *        call.
   * @param topCut communication nodes representing the top cut of the
   *        {@link EnactmentGraph}.
   * @param bottomCut communication nodes representing the bottom cut of the
   *        {@link EnactmentGraph}.
   *
   * @return the resulting cut out {@link EnactmentGraph} in the adapted
   *         {@link EnactmentSpecification}.
   *
   * @throws IllegalArgumentException if the cut is invalid.
   */
  public EnactmentSpecification cut(final EnactmentSpecification enactmentSpecification, final Set<Task> topCut,
      final Set<Task> bottomCut) {

    final EnactmentGraph eGraph = enactmentSpecification.getEnactmentGraph();

    // Validate the top and bottom cuts
    validateInput(eGraph, topCut, bottomCut);

    // Create the cut out graph
    final EnactmentGraph cutOutGraph = cutGraph(eGraph, topCut, bottomCut);

    // Insert new distributed engine node
    final String functionNodeId = insertFunctionNode(enactmentSpecification, topCut, bottomCut);

    // Remove outsourced edges, vertices and mappings from the
    // initial graph and add mappings of the cut out graph.
    final MappingsConcurrent mappingsCutOutGraph = new MappingsConcurrent();
    final MappingsConcurrent mappings = enactmentSpecification.getMappings();
    cutOutGraph.getEdges().forEach((edge) -> {

      // Check if already deleted
      try { eGraph.removeEdge(eGraph.getEdge(edge.getId())); } catch(IllegalArgumentException ignore) {}
    });
    cutOutGraph.getVertices().forEach((vertice) -> {
      if(!(topCut.contains(vertice) || bottomCut.contains(vertice))) {
        if(!vertice.getId().contains("result") && !vertice.getId().contains("comm")){ // TODO replace this if with a contains
          Set<Mapping<Task, Resource>> map = mappings.getMappings(vertice);
          map.forEach(mappingsCutOutGraph::addMapping);
          map.forEach(mappings::removeMapping);
        }
        eGraph.removeVertex(eGraph.getVertex(vertice.getId()));
      }
    });

    // Mark leaf and root nodes of the cut out graph and adapt input and output
    topCut.forEach((tTask) -> {
      PropertyServiceData.makeRoot(cutOutGraph.getVertex(tTask.getId()));
      final Dependency edge = eGraph.getEdge(tTask.getId() + "--" + functionNodeId);
      PropertyServiceDependency.setJsonKey(edge, PropertyServiceDependency.getJsonKey(edge) + "_" + tTask.getId());
      PropertyServiceData.setJsonKey(cutOutGraph.getVertex(tTask.getId()), PropertyServiceDependency.getJsonKey(edge));
    });
    bottomCut.forEach((bTask) -> {
      PropertyServiceData.makeLeaf(cutOutGraph.getVertex(bTask.getId()));
      final Dependency edge = eGraph.getEdge(functionNodeId + "--" + bTask.getId());
      PropertyServiceDependency.setJsonKey(edge, PropertyServiceDependency.getJsonKey(edge) + "_" + bTask.getId());
      PropertyServiceData.setJsonKey(cutOutGraph.getVertex(bTask.getId()), PropertyServiceDependency.getJsonKey(edge));
    });

    // Create the enactment specification of the cut out graph
    final EnactmentSpecification resultEnactmentSpecification = new EnactmentSpecification(
        cutOutGraph,
        enactmentSpecification.getResourceGraph(),
        mappingsCutOutGraph,
        UUID.randomUUID().toString());

    // Create new communication nodes for specification and configuration
    addCommunicationNode(eGraph, "Constant/" + Utility.SPECIFICATION + "_" + functionNodeId,
        prepareNodeConstantString(Utility.fromEnactmentSpecificationToString(resultEnactmentSpecification)),
        eGraph.getVertex(functionNodeId), Utility.SPECIFICATION);
    addCommunicationNode(eGraph, "Constant/" + Utility.CONFIGURATION + "_" + functionNodeId,
        prepareNodeConstantString(Utility.DE_CONFIGURATION),
        eGraph.getVertex(functionNodeId), Utility.CONFIGURATION);

    return resultEnactmentSpecification;
  }

  /**
   * Insert a function node and remap dependencies.
   *
   * @param enactmentSpecification the reference to the original input
   *         {@link EnactmentSpecification}.
   *         The {@link EnactmentGraph} will be adapted and contains
   *         the distributed engine node after this method call.
   * @param topCut communication nodes representing the top cut of the
   *        {@link EnactmentGraph}.
   * @param bottomCut communication nodes representing the bottom cut
   *        of the {@link EnactmentGraph}.
   *
   * @return the identifier of the newly created function node.
   */
  private String insertFunctionNode(final EnactmentSpecification enactmentSpecification, final Set<Task> topCut, final Set<Task> bottomCut) {
    final EnactmentGraph eGraph = enactmentSpecification.getEnactmentGraph();

    // Create and insert the function node for the distributed engine
    final String functionNodeId = topCut.toString() + bottomCut.toString();
    final Task functionNode = PropertyServiceFunctionUser.createUserTask(functionNodeId, Utility.DE_TYPE_ID);
    PropertyServiceFunction.setUsageType(PropertyServiceFunction.UsageType.User, functionNode);
    eGraph.addVertex(functionNode);

    // Add the distributed engine resource
    final Resource distributedEngineResource = PropertyServiceResourceServerless.createServerlessResource(Utility.DE_AWS_US_EAST_1,  Utility.DE_AWS_US_EAST_1);
    final ResourceGraph rGraph = enactmentSpecification.getResourceGraph();
    rGraph.addVertex(distributedEngineResource);
    final Resource engineResource = rGraph.getVertex(Utility.ENGINE);
    if(engineResource != null) {
      if(!rGraph.containsEdge(Utility.ENGINE + "--" + Utility.DE_AWS_US_EAST_1)){
        rGraph.addEdge(new Link(Utility.ENGINE + "--" + Utility.DE_AWS_US_EAST_1),
            rGraph.getVertex(Utility.ENGINE), distributedEngineResource,
            EdgeType.DIRECTED);
      }
    }

    // Add mapping for the distributed engine
    enactmentSpecification.getMappings().addMapping(PropertyServiceMapping.createMapping(eGraph.getVertex(functionNodeId), distributedEngineResource, PropertyServiceMapping.EnactmentMode.Serverless, Utility.DE_AWS_US_EAST_1));

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

    return functionNodeId;
  }

  /**
   * Create and add a new communication node the the {@link EnactmentGraph}.
   *
   * @param eGraph the {@link EnactmentGraph} to add the node to.
   * @param communicationId the identifier of the communication node.
   * @param content the content of the constant communication node.
   * @param toNode the node to connect the communication node with.
   * @param jsonKey the jsonKey of the communication node.
   */
  private void addCommunicationNode(final EnactmentGraph eGraph, final String communicationId,
      final String content, final Task toNode, final String jsonKey) {

    // Create communication node
    final Communication communication = new Communication(communicationId);
    PropertyServiceData.setContent(communication, new JsonPrimitive(content));
    PropertyServiceData.setDataType(communication, PropertyServiceData.DataType.String);
    PropertyServiceData.setNodeType(communication, PropertyServiceData.NodeType.Constant);

    // Create dependency
    final Dependency dependency = new Dependency(communicationId + "--" + toNode);
    PropertyServiceDependency.setJsonKey(dependency, jsonKey);
    PropertyServiceDependency.setType(dependency, PropertyServiceDependency.TypeDependency.Data);

    // Add dependency
    PropertyServiceDependency.addDataDependency(communication, toNode,
        PropertyServiceDependency.getJsonKey(dependency), eGraph);
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
  public boolean isCutValid(final EnactmentGraph eGraph, final Set<Task> topCut, final Set<Task> bottomCut) {
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
  private EnactmentGraph  cutGraph(final EnactmentGraph eGraph, final Set<Task> startCut, final Set<Task> endCut) {
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

  /**
   * Prepare the string for the enactment node constants.
   *
   * @param input the input string to prepare.
   *
   * @return the final prepared string.
   */
  private String prepareNodeConstantString(final String input) {
    return "\"" + input.replace("\"", "'").replaceAll("[\\t\\n\\r]+","") + "\"";
  }
}
