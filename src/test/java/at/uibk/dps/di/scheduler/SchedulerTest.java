package at.uibk.dps.di.scheduler;

import at.uibk.dps.di.constants.EnactmentGraphs;
import at.uibk.dps.di.properties.PropertyServiceScheduler;
import at.uibk.dps.ee.io.resources.ResourceGraphProviderFile;
import at.uibk.dps.ee.io.spec.SpecificationProviderFile;
import at.uibk.dps.ee.model.graph.*;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;
import org.junit.jupiter.api.Test;

import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.assertEquals;

/**
 * Test the workflow evaluator / scheduler.
 *
 * @author Stefan Pedtascher
 */
public class SchedulerTest {

    private final String localResourceName = "Enactment Engine (Local Machine)";
    private final String cloudResourceName = "https://fkwvdybi0a.execute-api.us-east-1.amazonaws.com/default/functino_noop_pub";

    /**
     * Setup the specification and add function durations and resource latency.
     *
     * @param eGraph the graph to generate the specification from.
     *
     * @return the generated specification.dd
     */
    private EnactmentSpecification setupSpecification(EnactmentGraph eGraph) {

        // Generate the specification
        final EnactmentGraphProvider eGraphProvider = () -> eGraph;
        String mappingsPath = Objects.requireNonNull(getClass().getClassLoader().getResource("mapping.json")).getPath();
        final ResourceGraphProvider rGraphProv = new ResourceGraphProviderFile(mappingsPath);
        final SpecificationProviderFile specProv = new SpecificationProviderFile(eGraphProvider, rGraphProv, mappingsPath);
        final EnactmentSpecification specification = specProv.getSpecification();

        // Set up resource instances and latency
        Resource local = specification.getResourceGraph().getVertex(localResourceName);
        PropertyServiceScheduler.setLatencyLocal(local, 0.0);
        PropertyServiceScheduler.setLatencyGlobal(local, 0.0);
        PropertyServiceScheduler.setInstances(local, 2);
        Resource noop = specification.getResourceGraph().getVertex(cloudResourceName);
        PropertyServiceScheduler.setLatencyLocal(noop, 200.0);
        PropertyServiceScheduler.setLatencyGlobal(noop, 500.0);
        PropertyServiceScheduler.setInstances(noop, 1000);

        // Set up function durations
        MappingsConcurrent mappings = specification.getMappings();
        mappings.mappingStream().forEach((map) -> PropertyServiceScheduler.setDuration(map, 2000.0));

        return specification;
    }

    @Test
    void testEvaluate() {
        EnactmentGraph eGraph = EnactmentGraphs.getComplexEnactmentGraph();
        final EnactmentSpecification specification = setupSpecification(eGraph);

        List<Cut> cuts = new Scheduler().schedule(specification);

        Iterator<Task> topCut1 = cuts.get(0).getTopCut().iterator();
        assertEquals("commNode17", topCut1.next().getId());
        assertEquals("commNode16", topCut1.next().getId());
        Iterator<Task> bottomCut1 = cuts.get(0).getBottomCut().iterator();
        assertEquals("commNode22", bottomCut1.next().getId());

        Iterator<Task> topCut2 = cuts.get(1).getTopCut().iterator();
        assertEquals("commNode8", topCut2.next().getId());
        Iterator<Task> bottomCut2 = cuts.get(1).getBottomCut().iterator();
        assertEquals("commNode19", bottomCut2.next().getId());

        Iterator<Task> topCut3 = cuts.get(2).getTopCut().iterator();
        assertEquals("commNode11", topCut3.next().getId());
        Iterator<Task> bottomCut3 = cuts.get(2).getBottomCut().iterator();
        assertEquals("commNode18", bottomCut3.next().getId());
   }
}
