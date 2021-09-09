package at.uibk.dps.di.constants;

import at.uibk.dps.ee.model.graph.EnactmentGraph;
import at.uibk.dps.ee.model.properties.PropertyServiceData;
import at.uibk.dps.ee.model.properties.PropertyServiceDependency;
import com.google.gson.JsonPrimitive;
import net.sf.opendse.model.Communication;
import net.sf.opendse.model.Task;

/**
 * {@link EnactmentGraph}s used for testing.
 *
 * @author Stefan Pedratscher
 */
public class EnactmentGraphs {

    /**
     * Get a simple medium sized {@link EnactmentGraph}. The medium sized {@link EnactmentGraph} consists
     * of 6 task nodes (tX) and 8 communication nodes (cX). It contains parallel and sequential nodes.
     *
     * Graphical representation of the medium sized {@link EnactmentGraph}:
     *
     *       c1
     *       |
     *       t1
     *      /  \
     *     c3  c2
     *     |   |
     *     t3  t2
     *     |   |
     *     c5  c4
     *     |   |
     *     t5  t4
     *     |   |
     *     c7  c6
     *      \ /
     *      t6
     *      |
     *      c8
     *
     *
     * @return the medium sized Enactment Graph.
     */
    public static EnactmentGraph getMediumSizedEnactmentGraph(){
        final Task comm1 = new Communication("commNode1");
        final Task comm2 = new Communication("commNode2");
        final Task comm3 = new Communication("commNode3");
        final Task comm4 = new Communication("commNode4");
        final Task comm5 = new Communication("commNode5");
        final Task comm6 = new Communication("commNode6");
        final Task comm7 = new Communication("commNode7");
        final Task comm8 = new Communication("commNode8");
        final Task task1 = new Task("taskNode1");
        final Task task2 = new Task("taskNode2");
        final Task task3 = new Task("taskNode3");
        final Task task4 = new Task("taskNode4");
        final Task task5 = new Task("taskNode5");
        final Task task6 = new Task("taskNode6");
        EnactmentGraph graph = new EnactmentGraph();
        PropertyServiceData.setContent(comm1, new JsonPrimitive(true));
        PropertyServiceDependency.addDataDependency(comm1, task1, "key1", graph);
        PropertyServiceDependency.addDataDependency(task1, comm2, "key2", graph);
        PropertyServiceDependency.addDataDependency(task1, comm3, "key3", graph);
        PropertyServiceDependency.addDataDependency(comm2, task2, "key4", graph);
        PropertyServiceDependency.addDataDependency(comm3, task3, "key5", graph);
        PropertyServiceDependency.addDataDependency(task2, comm4, "key6", graph);
        PropertyServiceDependency.addDataDependency(task3, comm5, "key7", graph);
        PropertyServiceDependency.addDataDependency(comm4, task4, "key8", graph);
        PropertyServiceDependency.addDataDependency(comm5, task5, "key9", graph);
        PropertyServiceDependency.addDataDependency(task4, comm6, "key10", graph);
        PropertyServiceDependency.addDataDependency(task5, comm7, "key11", graph);
        PropertyServiceDependency.addDataDependency(comm6, task6, "key12", graph);
        PropertyServiceDependency.addDataDependency(comm7, task6, "key13", graph);
        PropertyServiceDependency.addDataDependency(task6, comm8, "key14", graph);
        return graph;
    }

    /**
     * Get a {@link EnactmentGraph} with more complex data flow. The {@link EnactmentGraph} consists
     * of 2 task nodes (tX) and 5 communication nodes (cX). It contains parallel and sequential nodes.
     *
     * Graphical representation of the {@link EnactmentGraph} with more complex data flow:
     *
     *       c3 c1 c2
     *        \ | /  \
     *         t1    t2
     *         |     |
     *         c4    c5
     *
     * @return the {@link EnactmentGraph} with more complex data flow.
     */
    public static EnactmentGraph getMoreComplexDataFlowEnactmentGraph() {
        final Task comm1 = new Communication("commNode1");
        final Task comm2 = new Communication("commNode2");
        final Task comm3 = new Communication("commNode3");
        final Task comm4 = new Communication("commNode4");
        final Task comm5 = new Communication("commNode5");
        final Task task1 = new Task("taskNode1");
        final Task task2 = new Task("taskNode2");
        EnactmentGraph graph = new EnactmentGraph();
        PropertyServiceData.setContent(comm1, new JsonPrimitive(true));
        PropertyServiceData.setContent(comm2, new JsonPrimitive("value"));
        PropertyServiceData.setContent(comm3, new JsonPrimitive(12));
        PropertyServiceDependency.addDataDependency(comm1, task1, "key1", graph);
        PropertyServiceDependency.addDataDependency(comm2, task1, "key2", graph);
        PropertyServiceDependency.addDataDependency(comm3, task1, "key3", graph);
        PropertyServiceDependency.addDataDependency(comm2, task2, "key4", graph);
        PropertyServiceDependency.addDataDependency(task1, comm4, "key5", graph);
        PropertyServiceDependency.addDataDependency(task2, comm5, "key6", graph);
        return graph;
    }
}
