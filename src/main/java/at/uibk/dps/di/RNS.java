package at.uibk.dps.di;

import at.uibk.dps.di.incision.Incision;
import at.uibk.dps.di.incision.Utility;
import at.uibk.dps.di.properties.PropertyServiceScheduler;
import at.uibk.dps.di.scheduler.Cut;
import at.uibk.dps.ee.deploy.run.ImplementationRunBare;
import at.uibk.dps.ee.io.afcl.AfclReader;
import at.uibk.dps.ee.io.resources.ResourceGraphProviderFile;
import at.uibk.dps.ee.io.spec.SpecificationProviderFile;
import at.uibk.dps.ee.model.graph.EnactmentGraphProvider;
import at.uibk.dps.ee.model.graph.EnactmentSpecification;
import at.uibk.dps.ee.model.graph.MappingsConcurrent;
import at.uibk.dps.ee.model.graph.ResourceGraphProvider;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

public class RNS {

    private final String localResourceName = "Enactment Engine (Local Machine)";
    private final String cloudResourceName = "https://gsyhelawyk.execute-api.us-east-1.amazonaws.com/default/dummy";

    private EnactmentSpecification setupSpecification(String eGraphPath, String mappingsPath) {

        // Generate the specification
        final EnactmentGraphProvider eGraphProvider = () -> new AfclReader(eGraphPath).getEnactmentGraph();
        final ResourceGraphProvider rGraphProv = new ResourceGraphProviderFile(mappingsPath);
        final SpecificationProviderFile specProv = new SpecificationProviderFile(eGraphProvider, rGraphProv, mappingsPath);
        final EnactmentSpecification specification = specProv.getSpecification();


        // Set up resource instances and latencies
        Resource local = specification.getResourceGraph().getVertex(localResourceName);
        PropertyServiceScheduler.setLatencyLocal(local, 0.0);
        PropertyServiceScheduler.setLatencyGlobal(local, 0.0);
        PropertyServiceScheduler.setInstances(local, 1);
        Resource noop = specification.getResourceGraph().getVertex(cloudResourceName);
        PropertyServiceScheduler.setLatencyLocal(noop, 200.0);
        PropertyServiceScheduler.setLatencyGlobal(noop, 500.0);
        PropertyServiceScheduler.setInstances(noop, 1000);

        // Set up function durations
        MappingsConcurrent mappings = specification.getMappings();
        mappings.mappingStream().forEach((map) -> PropertyServiceScheduler.setDuration(map, 2000.0));

        return specification;
    }

    private void run(String afclPath, String mappingsPath, String input) {

        // Get the eGraph and specification (including function durations, task mappings, latencies)
        EnactmentSpecification specification = setupSpecification(afclPath, mappingsPath);


        List<Cut> cuts = new ArrayList<>();
        Set<Task> topCut = new HashSet<>();
        topCut.add(specification.getEnactmentGraph().getVertex("[while/counter, longSeq/input, while, while--stopCondition/3]"));
        Set<Task> bottomCut = new HashSet<>();
        bottomCut.add(specification.getEnactmentGraph().getVertex("while/whileIt"));
        cuts.add(new Cut(topCut, bottomCut));

        //EnactmentGraphViewer.view(specification.getEnactmentGraph());

        // Cut the workflow at the given position
        for(Cut cut: cuts) {
            new Incision().cut(specification, cut.getTopCut(), cut.getBottomCut());
        }


        // Get the adapted specification as string
        String specificationAdapted = Utility.fromEnactmentSpecificationToString(specification);

        // Run the workflow
        //new ImplementationRunBare().implement(input, specificationAdapted, Utility.DE_CONFIGURATION);
    }


    public static void main(String[] args) {
        new RNS().run(
                "src/test/resources/wf1.yaml",
                "src/test/resources/wf1.json",
                "{'input': 'This is some text'}");
    }
}
