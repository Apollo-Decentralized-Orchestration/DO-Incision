package at.uibk.dps.di.incision;

import at.uibk.dps.di.resources.EnactmentGraphs;
import at.uibk.dps.ee.model.graph.EnactmentGraph;
import net.sf.opendse.model.Task;
import org.junit.jupiter.api.Test;

import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test the cutting process of a workflow.
 *
 * @author Stefan Pedtascher
 */
class IncisionTest {

    /**
     * Test cut of a medium sized {@link EnactmentGraph}. The medium sized {@link EnactmentGraph} consists
     * of 6 task nodes (tX) and 8 communication nodes (cX). It contains parallel and sequential nodes.
     *
     * Cut @ c2 c3 and c6 c7
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
    */
    @Test
    void cutTestMediumSizedEnactmentGraph() throws IllegalArgumentException {
        Incision incision = new Incision();

        EnactmentGraph full = EnactmentGraphs.getMediumSizedEnactmentGraph();

        // Specify the top and bottom edges to cut
        Set<Task> topCut = new HashSet<>();
        Set<Task> bottomCut = new HashSet<>();
        topCut.add(full.getVertex("commNode2"));
        topCut.add(full.getVertex("commNode3"));
        bottomCut.add(full.getVertex("commNode6"));
        bottomCut.add(full.getVertex("commNode7"));

        // Check if edges are present before the cut
        assertNotNull(full.getEdge("taskNode3--commNode5"));
        assertNotNull(full.getEdge("taskNode2--commNode4"));
        assertNotNull(full.getEdge("commNode5--taskNode5"));
        assertNotNull(full.getEdge("commNode4--taskNode4"));

        // Check if tasks are present before the cut
        assertNotNull(full.getVertex("taskNode2"));
        assertNotNull(full.getVertex("taskNode3"));
        assertNotNull(full.getVertex("commNode4"));
        assertNotNull(full.getVertex("commNode5"));
        assertNotNull(full.getVertex("taskNode4"));
        assertNotNull(full.getVertex("taskNode5"));

        // Check edge connection before the cut
        assertEquals(full.getVertex("taskNode3"), full.getDest(full.getEdge("commNode3--taskNode3")));
        assertEquals(full.getVertex("taskNode2"), full.getDest(full.getEdge("commNode2--taskNode2")));
        assertEquals(full.getVertex("taskNode5"), full.getSource(full.getEdge("taskNode5--commNode7")));
        assertEquals(full.getVertex("taskNode4"), full.getSource(full.getEdge("taskNode4--commNode6")));

        // Cut the workflow
        EnactmentGraph result = incision.cut(full, topCut, bottomCut);

        // Check if edges are deleted after the cut
        assertNull(full.getEdge("taskNode3--commNode5"));
        assertNull(full.getEdge("taskNode2--commNode4"));
        assertNull(full.getEdge("commNode5--taskNode5"));
        assertNull(full.getEdge("commNode4--taskNode4"));

        // Check if tasks are deleted after the cut
        assertNull(full.getVertex("taskNode2"));
        assertNull(full.getVertex("taskNode3"));
        assertNull(full.getVertex("commNode4"));
        assertNull(full.getVertex("commNode5"));
        assertNull(full.getVertex("taskNode4"));
        assertNull(full.getVertex("taskNode5"));

        // Check if tasks are in resulting cut out graph
        assertNotNull(result.getVertex("commNode2"));
        assertNotNull(result.getVertex("commNode3"));
        assertNotNull(result.getVertex("commNode4"));
        assertNotNull(result.getVertex("commNode5"));
        assertNotNull(result.getVertex("commNode6"));
        assertNotNull(result.getVertex("commNode7"));
        assertNotNull(result.getVertex("taskNode2"));
        assertNotNull(result.getVertex("taskNode3"));
        assertNotNull(result.getVertex("taskNode4"));
        assertNotNull(result.getVertex("taskNode5"));

        // Check if edges are in resulting cut out graph
        assertNotNull(result.getEdge("taskNode3--commNode5"));
        assertNotNull(result.getEdge("taskNode2--commNode4"));
        assertNotNull(result.getEdge("taskNode5--commNode7"));
        assertNotNull(result.getEdge("taskNode4--commNode6"));
        assertNotNull(result.getEdge("commNode3--taskNode3"));
        assertNotNull(result.getEdge("commNode2--taskNode2"));
        assertNotNull(result.getEdge("commNode5--taskNode5"));
        assertNotNull(result.getEdge("commNode4--taskNode4"));

        // Check if leaf and root nodes are set
        assertTrue((boolean) result.getVertex("commNode7").getAttribute("Leaf"));
        assertTrue((boolean) result.getVertex("commNode6").getAttribute("Leaf"));
        assertTrue((boolean) result.getVertex("commNode3").getAttribute("Root"));
        assertTrue((boolean) result.getVertex("commNode2").getAttribute("Root"));

        // Check if resulting cut out graph does not contain the nodes
        assertNull(result.getVertex("taskNode1"));
        assertNull(result.getVertex("commNode1"));
        assertNull(result.getVertex("commNode8"));
        assertNull(result.getVertex("taskNode6"));

        // check if distributed engine node exists and is connected
        assertNotNull(full.getVertex("[commNode3, commNode2][commNode7, commNode6]"));
        assertNotNull(full.getEdge("[commNode3, commNode2][commNode7, commNode6]--commNode6"));
        assertNotNull(full.getEdge("[commNode3, commNode2][commNode7, commNode6]--commNode7"));
        assertNotNull(full.getEdge("commNode3--[commNode3, commNode2][commNode7, commNode6]"));
        assertNotNull(full.getEdge("commNode2--[commNode3, commNode2][commNode7, commNode6]"));
    }

    /**
     * Check a valid cut of a medium sized {@link EnactmentGraph}. The medium sized {@link EnactmentGraph} consists
     * of 6 task nodes (tX) and 8 communication nodes (cX). It contains parallel and sequential nodes.
     *
     * Cut @ c2 c3 and c4 c5
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
     */
    @Test
    void checkValidCutMediumSizedEnactmentGraph() {
        EnactmentGraph full = EnactmentGraphs.getMediumSizedEnactmentGraph();

        Incision incision = new Incision();

        Set<Task> topCut = new HashSet<>();
        topCut.add(full.getVertex("commNode3"));
        topCut.add(full.getVertex("commNode2"));

        Set<Task> bottomCut = new HashSet<>();
        bottomCut.add(full.getVertex("commNode4"));
        bottomCut.add(full.getVertex("commNode5"));

        assertTrue(incision.isCutValid(full, topCut, bottomCut));
    }

    /**
     * Check an invalid cut of a medium sized {@link EnactmentGraph}. The medium sized {@link EnactmentGraph} consists
     * of 6 task nodes (tX) and 8 communication nodes (cX). It contains parallel and sequential nodes.
     *
     * Cut @ c2 c3 and c7
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
     */
    @Test
    void checkInvalidCutMediumSizedEnactmentGraph() {
        EnactmentGraph full = EnactmentGraphs.getMediumSizedEnactmentGraph();

        Incision incision = new Incision();

        Set<Task> topCut = new HashSet<>();
        topCut.add(full.getVertex("commNode3"));
        topCut.add(full.getVertex("commNode2"));

        Set<Task> bottomCut = new HashSet<>();
        bottomCut.add(full.getVertex("commNode7"));

        assertFalse(incision.isCutValid(full, topCut, bottomCut));
    }

    /**
     * Check an invalid cut of a {@link EnactmentGraph} with more complex data flow. The {@link EnactmentGraph} consists
     * of 2 task nodes (tX) and 5 communication nodes (cX). It contains parallel and sequential nodes.
     *
     * Cut @ c2 c3 and c4 c5
     *
     * Graphical representation of the {@link EnactmentGraph} with more complex data flow:
     *
     *       c3 c1 c2
     *        \ | /  \
     *         t1    t2
     *         |     |
     *         c4    c5
     */
    @Test
    void checkInvalidCutComplexDataFlowEnactmentGraph() {
        EnactmentGraph full = EnactmentGraphs.getMoreComplexDataFlowEnactmentGraph();

        Incision incision = new Incision();

        Set<Task> topCut = new HashSet<>();
        topCut.add(full.getVertex("commNode2"));
        topCut.add(full.getVertex("commNode3"));

        Set<Task> bottomCut = new HashSet<>();
        bottomCut.add(full.getVertex("commNode4"));
        bottomCut.add(full.getVertex("commNode5"));

        assertFalse(incision.isCutValid(full, topCut, bottomCut));
    }

    /*
    public static void main(String[] args) throws IllegalArgumentException {
        Incision cutter = new Incision();

        EnactmentGraph full = EnactmentGraphs.getMediumSizedEnactmentGraph();
        Set<Task> topNodes = new HashSet<>();
        topNodes.add(full.getVertex("commNode3"));
        topNodes.add(full.getVertex("commNode2"));

        Set<Task> bottomNodes = new HashSet<>();
        bottomNodes.add(full.getVertex("commNode7"));
        bottomNodes.add(full.getVertex("commNode6"));

        EnactmentGraph result;
        try {
            result = cutter.cut(full, topNodes, bottomNodes);
            EnactmentGraphViewer.view(full);
        } catch (IllegalArgumentException e) {
            e.printStackTrace();
        }
    }*/
}
