package at.uibk.dps.di.constants;

import at.uibk.dps.ee.model.graph.EnactmentGraph;
import at.uibk.dps.ee.model.properties.PropertyServiceData;
import at.uibk.dps.ee.model.properties.PropertyServiceDependency;
import at.uibk.dps.ee.model.properties.PropertyServiceFunctionUser;
import at.uibk.dps.ee.visualization.model.EnactmentGraphViewer;
import com.google.gson.JsonPrimitive;
import net.sf.opendse.model.Communication;
import net.sf.opendse.model.Task;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * {@link EnactmentGraph}s used for testing.
 *
 * @author Stefan Pedratscher
 */
public class EnactmentGraphs {

    public static EnactmentGraph getMediumSizedEnactmentGraph3(){
        final Task comm1 = new Communication("commNode1");
        final Task comm2 = new Communication("commNode2");
        final Task comm3 = new Communication("commNode3");
        final Task comm4 = new Communication("commNode4");
        final Task comm5 = new Communication("commNode5");
        final Task comm6 = new Communication("commNode6");
        final Task comm7 = new Communication("commNode7");
        final Task comm8 = new Communication("commNode8");
        final Task comm9 = new Communication("commNode9");
        final Task comm10 = new Communication("commNode10");
        final Task comm11 = new Communication("commNode11");
        final Task comm12 = new Communication("commNode12");
        final Task comm13 = new Communication("commNode13");
        final Task comm14 = new Communication("commNode14");
        final Task comm15 = new Communication("commNode15");
        final Task comm16 = new Communication("commNode16");
        final Task comm17 = new Communication("commNode17");
        final Task task1 = PropertyServiceFunctionUser.createUserTask("taskNode1", "noop");
        final Task task2 = PropertyServiceFunctionUser.createUserTask("taskNode2", "noop");
        final Task task3 = PropertyServiceFunctionUser.createUserTask("taskNode3", "noop");
        final Task task4 = PropertyServiceFunctionUser.createUserTask("taskNode4", "noop");
        final Task task5 = PropertyServiceFunctionUser.createUserTask("taskNode5", "noop");
        final Task task6 = PropertyServiceFunctionUser.createUserTask("taskNode6", "noop");
        final Task task7 = PropertyServiceFunctionUser.createUserTask("taskNode7", "noop");
        final Task task8 = PropertyServiceFunctionUser.createUserTask("taskNode8", "noop");
        final Task task9 = PropertyServiceFunctionUser.createUserTask("taskNode9", "noop");
        final Task task10 = PropertyServiceFunctionUser.createUserTask("taskNode10", "noop");
        final Task task11 = PropertyServiceFunctionUser.createUserTask("taskNode11", "noop");
        final Task task12 = PropertyServiceFunctionUser.createUserTask("taskNode12", "noop");
        final Task task13 = PropertyServiceFunctionUser.createUserTask("taskNode13", "noop");
        final Task task14 = PropertyServiceFunctionUser.createUserTask("taskNode14", "noop");
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
        PropertyServiceDependency.addDataDependency(comm7, task7, "key13", graph);
        PropertyServiceDependency.addDataDependency(task6, comm8, "key14", graph);
        PropertyServiceDependency.addDataDependency(task7, comm9, "key15", graph);
        PropertyServiceDependency.addDataDependency(comm8, task8, "key16", graph);
        PropertyServiceDependency.addDataDependency(comm9, task9, "key17", graph);
        PropertyServiceDependency.addDataDependency(task8, comm10, "key18", graph);
        PropertyServiceDependency.addDataDependency(task9, comm11, "key19", graph);
        PropertyServiceDependency.addDataDependency(comm10, task10, "key20", graph);
        PropertyServiceDependency.addDataDependency(comm11, task10, "key21", graph);
        PropertyServiceDependency.addDataDependency(task10, comm12, "key22", graph);

        PropertyServiceDependency.addDataDependency(task1, comm13, "key23", graph);
        PropertyServiceDependency.addDataDependency(comm13, task11, "key24", graph);
        PropertyServiceDependency.addDataDependency(task11, comm14, "key25", graph);
        PropertyServiceDependency.addDataDependency(comm14, task12, "key26", graph);
        PropertyServiceDependency.addDataDependency(task12, comm15, "key27", graph);
        PropertyServiceDependency.addDataDependency(comm15, task13, "key28", graph);
        PropertyServiceDependency.addDataDependency(task13, comm16, "key29", graph);
        PropertyServiceDependency.addDataDependency(comm16, task14, "key30", graph);
        PropertyServiceDependency.addDataDependency(task14, comm17, "key31", graph);
        PropertyServiceDependency.addDataDependency(comm17, task10, "key32", graph);
        PropertyServiceData.makeRoot(comm1);
        PropertyServiceData.makeLeaf(comm12);
        return graph;
    }

    public static EnactmentGraph getMediumSizedEnactmentGraph2(){
        final Task comm1 = new Communication("commNode1");
        final Task comm2 = new Communication("commNode2");
        final Task comm3 = new Communication("commNode3");
        final Task comm4 = new Communication("commNode4");
        final Task comm5 = new Communication("commNode5");
        final Task comm6 = new Communication("commNode6");
        final Task comm7 = new Communication("commNode7");
        final Task comm8 = new Communication("commNode8");
        final Task comm9 = new Communication("commNode9");
        final Task comm10 = new Communication("commNode10");
        final Task comm11 = new Communication("commNode11");
        final Task comm12 = new Communication("commNode12");
        final Task task1 = PropertyServiceFunctionUser.createUserTask("taskNode1", "noop");
        final Task task2 = PropertyServiceFunctionUser.createUserTask("taskNode2", "noop");
        final Task task3 = PropertyServiceFunctionUser.createUserTask("taskNode3", "noop");
        final Task task4 = PropertyServiceFunctionUser.createUserTask("taskNode4", "noop");
        final Task task5 = PropertyServiceFunctionUser.createUserTask("taskNode5", "noop");
        final Task task6 = PropertyServiceFunctionUser.createUserTask("taskNode6", "noop");
        final Task task7 = PropertyServiceFunctionUser.createUserTask("taskNode7", "noop");
        final Task task8 = PropertyServiceFunctionUser.createUserTask("taskNode8", "noop");
        final Task task9 = PropertyServiceFunctionUser.createUserTask("taskNode9", "noop");
        final Task task10 = PropertyServiceFunctionUser.createUserTask("taskNode10", "noop");
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
        PropertyServiceDependency.addDataDependency(comm7, task7, "key13", graph);
        PropertyServiceDependency.addDataDependency(task6, comm8, "key14", graph);
        PropertyServiceDependency.addDataDependency(task7, comm9, "key15", graph);
        PropertyServiceDependency.addDataDependency(comm8, task8, "key16", graph);
        PropertyServiceDependency.addDataDependency(comm9, task9, "key17", graph);
        PropertyServiceDependency.addDataDependency(task8, comm10, "key18", graph);
        PropertyServiceDependency.addDataDependency(task9, comm11, "key19", graph);
        PropertyServiceDependency.addDataDependency(comm10, task10, "key20", graph);
        PropertyServiceDependency.addDataDependency(comm11, task10, "key21", graph);
        PropertyServiceDependency.addDataDependency(task10, comm12, "key22", graph);
        PropertyServiceData.makeRoot(comm1);
        PropertyServiceData.makeLeaf(comm12);
        return graph;
    }

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
        final Task task1 = PropertyServiceFunctionUser.createUserTask("taskNode1", "noop");
        final Task task2 = PropertyServiceFunctionUser.createUserTask("taskNode2", "noop");
        final Task task3 = PropertyServiceFunctionUser.createUserTask("taskNode3", "noop");
        final Task task4 = PropertyServiceFunctionUser.createUserTask("taskNode4", "noop");
        final Task task5 = PropertyServiceFunctionUser.createUserTask("taskNode5", "noop");
        final Task task6 = PropertyServiceFunctionUser.createUserTask("taskNode6", "noop");
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
        PropertyServiceData.makeRoot(comm1);
        PropertyServiceData.makeLeaf(comm8);
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
        final Task task1 = new Task("taskNode1"); task1.setAttribute("Duration", 2000.0);
        final Task task2 = new Task("taskNode2"); task2.setAttribute("Duration", 2000.0);
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

    /**
     * Get a complex {@link EnactmentGraph} with complex data flow. The {@link EnactmentGraph} consists
     * of 18 task nodes (tX) and 20 communication nodes (cX). It contains parallel and sequential nodes.
     *
     * Graphical representation of the {@link EnactmentGraph} is to complex. Use the
     * EnactmentGraphViewer to view the graph.
     *
     * @return the {@link EnactmentGraph}.
     */
    public static EnactmentGraph getComplexEnactmentGraph() {
        final List<Task> communicationNodes = new ArrayList<>();
        IntStream.rangeClosed(1, 25).forEach((i) -> communicationNodes.add(new Communication("commNode" + i)));

        final List<Task> taskNodes = new ArrayList<>();
        IntStream.rangeClosed(1, 18).forEach((i) -> {  final Task task = new Task("taskNode" + i); task.setAttribute("Duration", 2000.0); task.setAttribute("UsageType", "User"); task.setAttribute("TypeID", "noop"); taskNodes.add(task); });

        EnactmentGraph graph = new EnactmentGraph();
        PropertyServiceData.setContent(communicationNodes.get(0), new JsonPrimitive(true));
        PropertyServiceDependency.addDataDependency(communicationNodes.get(0), taskNodes.get(0), "key1", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(1), taskNodes.get(1), "key2", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(2), taskNodes.get(2), "key3", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(3), taskNodes.get(7), "key4", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(4), taskNodes.get(3), "key5", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(5), taskNodes.get(4), "key6", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(6), taskNodes.get(5), "key7", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(7), taskNodes.get(6), "key8", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(8), taskNodes.get(7), "key9", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(9), taskNodes.get(8), "key10", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(10), taskNodes.get(9), "key11", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(11), taskNodes.get(10), "key12", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(12), taskNodes.get(11), "key13", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(13), taskNodes.get(12), "key14", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(14), taskNodes.get(13), "key15", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(15), taskNodes.get(14), "key16", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(16), taskNodes.get(14), "key17", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(17), taskNodes.get(15), "key18", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(18), taskNodes.get(15), "key19", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(19), taskNodes.get(15), "key20", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(20), taskNodes.get(15), "key21", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(21), taskNodes.get(15), "key22", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(22), taskNodes.get(16), "key23", graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get(22), taskNodes.get(17), "key23", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(0), communicationNodes.get(1), "key24", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(0), communicationNodes.get(2), "key25", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(1), communicationNodes.get(3), "key26", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(2), communicationNodes.get(4), "key27", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(3), communicationNodes.get(5), "key28", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(3), communicationNodes.get(6), "key29", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(3), communicationNodes.get(7), "key30", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(4), communicationNodes.get(8), "key31", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(4), communicationNodes.get(9), "key32", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(5), communicationNodes.get(10), "key33", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(6), communicationNodes.get(11), "key34", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(7), communicationNodes.get(12), "key35", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(8), communicationNodes.get(16), "key36", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(9), communicationNodes.get(17), "key37", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(10), communicationNodes.get(18), "key38", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(11), communicationNodes.get(13), "key39", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(11), communicationNodes.get(14), "key40", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(11), communicationNodes.get(15), "key41", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(12), communicationNodes.get(19), "key42", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(13), communicationNodes.get(20), "key43", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(14), communicationNodes.get(21), "key44", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(15), communicationNodes.get(22), "key45", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(16), communicationNodes.get(23), "key46", graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(17), communicationNodes.get(24), "key46", graph);
        PropertyServiceData.makeRoot(communicationNodes.get(0));
        PropertyServiceData.makeLeaf(communicationNodes.get(23));
        PropertyServiceData.makeLeaf(communicationNodes.get(24));

        return graph;
    }

    /**
     * Get a {@link EnactmentGraph} with overlapping data flow. The {@link EnactmentGraph} consists
     * of 7 task nodes (tX) and 8 communication nodes (cX). It contains parallel and sequential nodes.
     *
     * Graphical representation of the {@link EnactmentGraph}:
     *
     *       c1
     *       |
     *       t1
     *       |
     *       c2
     *       |
     *       t2
     *       |
     *       c3_____
     *       | \ \  \
     *       t3 \ \  \
     *       |   | | |
     *       c4  | | |
     *       |   | | |
     *       t4  | | |
     *       |   / | |
     *       c5 /  | |
     *       | /   / |
     *       t5   /  |
     *       |   /   /
     *       c6 /   /
     *       | /   /
     *       t6   /
     *       |   /
     *       c7 /
     *       | /
     *       t7
     *       |
     *       c8
     *
     *
     * @return the {@link EnactmentGraph}.
     */
    public static EnactmentGraph getEnactmentGraphOverlappingDataFlow() {
        final Task comm1 = new Communication("commNode1");
        final Task comm2 = new Communication("commNode2");
        final Task comm3 = new Communication("commNode3");
        final Task comm4 = new Communication("commNode4");
        final Task comm5 = new Communication("commNode5");
        final Task comm6 = new Communication("commNode6");
        final Task comm7 = new Communication("commNode7");
        final Task comm8 = new Communication("commNode8");
        final Task task1 = new Task("taskNode1"); task1.setAttribute("Duration", 2000.0);
        final Task task2 = new Task("taskNode2"); task2.setAttribute("Duration", 2000.0);
        final Task task3 = new Task("taskNode3"); task3.setAttribute("Duration", 2000.0);
        final Task task4 = new Task("taskNode4"); task4.setAttribute("Duration", 2000.0);
        final Task task5 = new Task("taskNode5"); task5.setAttribute("Duration", 2000.0);
        final Task task6 = new Task("taskNode6"); task6.setAttribute("Duration", 2000.0);
        final Task task7 = new Task("taskNode7"); task7.setAttribute("Duration", 2000.0);
        EnactmentGraph graph = new EnactmentGraph();
        PropertyServiceData.setContent(comm1, new JsonPrimitive(true));
        PropertyServiceDependency.addDataDependency(comm1, task1, "key1", graph);
        PropertyServiceDependency.addDataDependency(task1, comm2, "key1", graph);
        PropertyServiceDependency.addDataDependency(comm2, task2, "key2", graph);
        PropertyServiceDependency.addDataDependency(task2, comm3, "key2", graph);
        PropertyServiceDependency.addDataDependency(comm3, task3, "key3", graph);
        PropertyServiceDependency.addDataDependency(task3, comm4, "key3", graph);
        PropertyServiceDependency.addDataDependency(comm4, task4, "key4", graph);
        PropertyServiceDependency.addDataDependency(task4, comm5, "key4", graph);
        PropertyServiceDependency.addDataDependency(comm5, task5, "key5", graph);
        PropertyServiceDependency.addDataDependency(task5, comm6, "key5", graph);
        PropertyServiceDependency.addDataDependency(comm6, task6, "key6", graph);
        PropertyServiceDependency.addDataDependency(task6, comm7, "key6", graph);
        PropertyServiceDependency.addDataDependency(comm7, task7, "key7", graph);
        PropertyServiceDependency.addDataDependency(task7, comm8, "key7", graph);
        PropertyServiceDependency.addDataDependency(comm3, task5, "key8", graph);
        PropertyServiceDependency.addDataDependency(comm3, task6, "key9", graph);
        PropertyServiceDependency.addDataDependency(comm3, task7, "key10", graph);
        PropertyServiceData.makeRoot(comm1);
        PropertyServiceData.makeLeaf(comm8);
        return graph;
    }
}
