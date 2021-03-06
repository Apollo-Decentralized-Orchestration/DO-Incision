package at.uibk.dps.di.incision;

import at.uibk.dps.di.constants.EnactmentGraphs;
import at.uibk.dps.ee.deploy.run.ImplementationRunBare;
import at.uibk.dps.ee.io.resources.ResourceGraphProviderFile;
import at.uibk.dps.ee.io.spec.SpecificationProviderFile;
import at.uibk.dps.ee.model.graph.*;
import net.sf.opendse.model.Task;
import nu.xom.ParsingException;
import org.junit.jupiter.api.Test;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.diff.DefaultNodeMatcher;
import org.xmlunit.diff.Diff;
import org.xmlunit.diff.ElementSelectors;

import java.io.IOException;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

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

        EnactmentGraph eGraph = EnactmentGraphs.getMediumSizedEnactmentGraph();

        // Specify the top and bottom edges to cut
        Set<Task> topCut = new HashSet<>();
        Set<Task> bottomCut = new HashSet<>();
        topCut.add(eGraph.getVertex("commNode2"));
        topCut.add(eGraph.getVertex("commNode3"));
        bottomCut.add(eGraph.getVertex("commNode6"));
        bottomCut.add(eGraph.getVertex("commNode7"));

        // Check if edges are present before the cut
        assertNotNull(eGraph.getEdge("taskNode3--commNode5"));
        assertNotNull(eGraph.getEdge("taskNode2--commNode4"));
        assertNotNull(eGraph.getEdge("commNode5--taskNode5"));
        assertNotNull(eGraph.getEdge("commNode4--taskNode4"));

        // Check if tasks are present before the cut
        assertNotNull(eGraph.getVertex("taskNode2"));
        assertNotNull(eGraph.getVertex("taskNode3"));
        assertNotNull(eGraph.getVertex("commNode4"));
        assertNotNull(eGraph.getVertex("commNode5"));
        assertNotNull(eGraph.getVertex("taskNode4"));
        assertNotNull(eGraph.getVertex("taskNode5"));

        // Check edge connection before the cut
        assertEquals(eGraph.getVertex("taskNode3"), eGraph.getDest(eGraph.getEdge("commNode3--taskNode3")));
        assertEquals(eGraph.getVertex("taskNode2"), eGraph.getDest(eGraph.getEdge("commNode2--taskNode2")));
        assertEquals(eGraph.getVertex("taskNode5"), eGraph.getSource(eGraph.getEdge("taskNode5--commNode7")));
        assertEquals(eGraph.getVertex("taskNode4"), eGraph.getSource(eGraph.getEdge("taskNode4--commNode6")));

        // Cut the workflow
        final EnactmentGraphProvider eGraphProvider = () -> eGraph;
        final ResourceGraphProvider rGraphProv = new ResourceGraphProviderFile(Objects.requireNonNull(getClass().getClassLoader().getResource("mapping.json")).getPath());
        final SpecificationProviderFile specProv = new SpecificationProviderFile(eGraphProvider, rGraphProv, Objects.requireNonNull(getClass().getClassLoader().getResource("mapping.json")).getPath());
        final EnactmentSpecification spec = specProv.getSpecification();
        EnactmentSpecification resultEnactmentSpecification = incision.cut(spec, topCut, bottomCut);
        EnactmentGraph result = resultEnactmentSpecification.getEnactmentGraph();

        // Check if edges are deleted after the cut
        assertThrows(IllegalArgumentException.class, () -> eGraph.getEdge("taskNode3--commNode5"));
        assertThrows(IllegalArgumentException.class, () -> eGraph.getEdge("taskNode2--commNode4"));
        assertThrows(IllegalArgumentException.class, () -> eGraph.getEdge("commNode5--taskNode5"));
        assertThrows(IllegalArgumentException.class, () -> eGraph.getEdge("commNode4--taskNode4"));

        // Check if tasks are deleted after the cut
        assertThrows(IllegalStateException.class, () -> eGraph.getVertex("taskNode2"));
        assertThrows(IllegalStateException.class, () -> eGraph.getVertex("taskNode3"));
        assertThrows(IllegalStateException.class, () -> eGraph.getVertex("commNode4"));
        assertThrows(IllegalStateException.class, () -> eGraph.getVertex("commNode5"));
        assertThrows(IllegalStateException.class, () -> eGraph.getVertex("taskNode4"));
        assertThrows(IllegalStateException.class, () -> eGraph.getVertex("taskNode5"));

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
        assertThrows(IllegalStateException.class, () -> result.getVertex("taskNode1"));
        assertThrows(IllegalStateException.class, () -> result.getVertex("commNode1"));
        assertThrows(IllegalStateException.class, () -> result.getVertex("commNode8"));
        assertThrows(IllegalStateException.class, () -> result.getVertex("taskNode6"));

        // check if distributed engine node exists and is connected
        assertNotNull(eGraph.getVertex("[commNode3, commNode2][commNode7, commNode6]"));
        assertNotNull(eGraph.getEdge("[commNode3, commNode2][commNode7, commNode6]--commNode6"));
        assertNotNull(eGraph.getEdge("[commNode3, commNode2][commNode7, commNode6]--commNode7"));
        assertNotNull(eGraph.getEdge("commNode3--[commNode3, commNode2][commNode7, commNode6]"));
        assertNotNull(eGraph.getEdge("commNode2--[commNode3, commNode2][commNode7, commNode6]"));
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

        EnactmentSpecification enactmentSpecification = new EnactmentSpecification(full, new ResourceGraph(), new MappingsConcurrent(), UUID.randomUUID().toString());
        assertThrows(IllegalArgumentException.class, () -> incision.cut(enactmentSpecification, topCut, bottomCut));
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
        EnactmentGraph eGraph = EnactmentGraphs.getMoreComplexDataFlowEnactmentGraph();

        Incision incision = new Incision();

        Set<Task> topCut = new HashSet<>();
        topCut.add(eGraph.getVertex("commNode2"));
        topCut.add(eGraph.getVertex("commNode3"));

        Set<Task> bottomCut = new HashSet<>();
        bottomCut.add(eGraph.getVertex("commNode4"));
        bottomCut.add(eGraph.getVertex("commNode5"));

        assertFalse(incision.isCutValid(eGraph, topCut, bottomCut));

        EnactmentSpecification enactmentSpecification = new EnactmentSpecification(eGraph, new ResourceGraph(), new MappingsConcurrent(), UUID.randomUUID().toString());
        assertThrows(IllegalArgumentException.class, () -> incision.cut(enactmentSpecification, topCut, bottomCut));
    }

    /**
     * Check an invalid cut (missing top cut) of a {@link EnactmentGraph} with more complex data flow.
     * The {@link EnactmentGraph} consists of 2 task nodes (tX) and 5 communication nodes (cX). It
     * contains parallel and sequential nodes.
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
    void checkMissingTopCut(){
        EnactmentGraph eGraph = EnactmentGraphs.getMoreComplexDataFlowEnactmentGraph();
        Incision incision = new Incision();

        Set<Task> topCut = new HashSet<>();

        Set<Task> bottomCut = new HashSet<>();
        bottomCut.add(eGraph.getVertex("commNode4"));
        bottomCut.add(eGraph.getVertex("commNode5"));

        EnactmentSpecification enactmentSpecification = new EnactmentSpecification(eGraph, new ResourceGraph(), new MappingsConcurrent(), UUID.randomUUID().toString());
        assertThrows(IllegalArgumentException.class, () -> incision.cut(enactmentSpecification, topCut, bottomCut));
    }

    /**
     * Check the cut of an {@link EnactmentSpecification} from an external AFCL mapping file. The
     * Workflow consists of a sequence of 6 task nodes (tX) and 7 communication nodes (cX).
     *
     * Graphical representation of the {@link EnactmentGraph}:
     *
     *  c0
     *  |
     *  t1
     *  |
     *  c1
     *  |
     *  t2
     *  |
     *  c2
     *  |
     *  t3
     *  |
     *  c3
     *  |
     *  t4
     *  |
     *  c4
     *  |
     *  t5
     *  |
     *  c5
     *  |
     *  t6
     *  |
     *  c6
     *
     * @throws ParsingException on parsing failure.
     * @throws IOException on io failure.
     */
    @Test void checkCutFromAFCL() throws ParsingException, IOException {
        String specificationFromAFCL = Utility.specFromAFCL(
            Objects.requireNonNull(getClass().getClassLoader().getResource("workflow.yaml")).getPath(),
            Objects.requireNonNull(getClass().getClassLoader().getResource("mapping.json")).getPath());

        EnactmentSpecification enactmentSpecification = Utility.fromStringToEnactmentSpecification(specificationFromAFCL);
        EnactmentGraph eGraph = enactmentSpecification.getEnactmentGraph();

        Incision incision = new Incision();
        Set<Task> topCut = new HashSet<>();
        topCut.add(eGraph.getVertex("noop1/result"));
        Set<Task> bottomCut = new HashSet<>();
        bottomCut.add(eGraph.getVertex("noop5/result"));

        EnactmentSpecification cutOutGraph = incision.cut(enactmentSpecification, topCut, bottomCut);

        String specificationAdapted = Utility.fromEnactmentSpecificationToString(enactmentSpecification);

        Diff difference = DiffBuilder.compare(specificationFromAFCL)
            .withTest(specificationAdapted)
            .withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndAllAttributes))
            .checkForSimilar()
            .build();

        assertTrue(difference.hasDifferences());

        difference = DiffBuilder.compare(specificationFromAFCL)
            .withTest(Utility.fromEnactmentSpecificationToString(cutOutGraph))
            .withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndAllAttributes))
            .checkForSimilar()
            .build();

        assertTrue(difference.hasDifferences());

        //new ImplementationRunBare().implement("{ \"input.json\" : \"3\" }", specificationAdapted, Utility.DE_CONFIGURATION);
    }

    /**
     * Test multiple inputs and outputs on the cut out graph. The tested graph consists of
     * 7 task nodes (tX) and 8 communication nodes (cX).
     *
     * Cut @ c1 c2 and c3 c4
     *
     * Graphical representation of the {@link EnactmentGraph}:
     *
     *       c0
     *      / \
     *     t1  t2
     *     |   |
     *     c1  c2
     *     |   |
     *     t3  t4
     *     |   |
     *     c3  c4
     *     |   |
     *     t5  t6
     *     |   |
     *     c5  c6
     *      \ /
     *      t7
     *      |
     *      c7
     */
    @Test
    void multipleInputsAndOutputs() throws ParsingException, IOException {
        String specificationFromAFCL = Utility.specFromAFCL(
            Objects.requireNonNull(getClass().getClassLoader().getResource("workflowMultipleInputs.yaml")).getPath(),
            Objects.requireNonNull(getClass().getClassLoader().getResource("mapping.json")).getPath());

        EnactmentSpecification enactmentSpecification = Utility.fromStringToEnactmentSpecification(specificationFromAFCL);
        EnactmentGraph eGraph = enactmentSpecification.getEnactmentGraph();

        Incision incision = new Incision();
        Set<Task> topCut = new HashSet<>();
        topCut.add(eGraph.getVertex("noop1/result"));
        topCut.add(eGraph.getVertex("noop2/result"));
        Set<Task> bottomCut = new HashSet<>();
        bottomCut.add(eGraph.getVertex("noop3/result"));
        bottomCut.add(eGraph.getVertex("noop4/result"));

        EnactmentGraph cutOutGraph = incision.cut(enactmentSpecification, topCut, bottomCut).getEnactmentGraph();

        assertEquals("input_noop1/result", cutOutGraph.getVertex("noop1/result").getAttribute("JsonKey"));
        assertEquals("input_noop2/result", cutOutGraph.getVertex("noop2/result").getAttribute("JsonKey"));
        assertEquals("result_noop3/result", cutOutGraph.getVertex("noop3/result").getAttribute("JsonKey"));
        assertEquals("result_noop4/result", cutOutGraph.getVertex("noop4/result").getAttribute("JsonKey"));

        assertEquals("input", cutOutGraph.getEdge("noop1/result--noop3").getAttribute("JsonKey"));
        assertEquals("input", cutOutGraph.getEdge("noop2/result--noop4").getAttribute("JsonKey"));
        assertEquals("result", cutOutGraph.getEdge("noop3--noop3/result").getAttribute("JsonKey"));
        assertEquals("result", cutOutGraph.getEdge("noop4--noop4/result").getAttribute("JsonKey"));

        assertEquals("input_noop1/result", eGraph.getEdge("noop1/result--[noop2/result, noop1/result][noop4/result, noop3/result]").getAttribute("JsonKey"));
        assertEquals("input_noop2/result", eGraph.getEdge("noop2/result--[noop2/result, noop1/result][noop4/result, noop3/result]").getAttribute("JsonKey"));
        assertEquals("result_noop3/result", eGraph.getEdge("[noop2/result, noop1/result][noop4/result, noop3/result]--noop3/result").getAttribute("JsonKey"));
        assertEquals("result_noop4/result", eGraph.getEdge("[noop2/result, noop1/result][noop4/result, noop3/result]--noop4/result").getAttribute("JsonKey"));
    }
}
