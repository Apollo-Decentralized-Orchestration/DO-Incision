package at.uibk.dps.di;

import java.util.ArrayList;
import java.util.List;

import at.uibk.dps.di.incision.Incision;
import at.uibk.dps.di.properties.PropertyServiceScheduler;
import at.uibk.dps.di.scheduler.Cut;
import at.uibk.dps.di.scheduler.Scheduler;
import at.uibk.dps.ee.io.resources.ResourceGraphProviderFile;
import at.uibk.dps.ee.io.spec.SpecificationProviderFile;
import at.uibk.dps.ee.model.graph.EnactmentGraph;
import at.uibk.dps.ee.model.graph.EnactmentGraphProvider;
import at.uibk.dps.ee.model.graph.EnactmentSpecification;
import at.uibk.dps.ee.model.graph.MappingsConcurrent;
import at.uibk.dps.ee.model.graph.ResourceGraphProvider;
import at.uibk.dps.ee.model.properties.PropertyServiceData;
import at.uibk.dps.ee.model.properties.PropertyServiceDependency;
import at.uibk.dps.ee.model.properties.PropertyServiceFunctionUser;
import at.uibk.dps.ee.visualization.model.EnactmentGraphViewer;
import net.sf.opendse.model.Communication;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;

public class Runner {
    public static EnactmentGraph getDynamicWf(int num){
        final List<Task> communicationNodes = new ArrayList<Task>();
        final List<Task> taskNodes = new ArrayList<Task>();

        String input = "in";
        String output = "out";

        for(int i = 0; i < (2 + 2 * num); i++) {
            communicationNodes.add(new Communication("commNode" + i));
            taskNodes.add(PropertyServiceFunctionUser.createUserTask("taskNode" + i, "Pass"));
        }
        communicationNodes.add(new Communication("commNode" + ((2 + 2 * num))));
        communicationNodes.add(new Communication("commNode" + ((2 + 2 * num) + 1)));

        EnactmentGraph graph = new EnactmentGraph();
        PropertyServiceDependency.addDataDependency(communicationNodes.get(0), taskNodes.get(0), input, graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(0), communicationNodes.get(1), output, graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get(0), communicationNodes.get(2), output, graph);

        for(int i = 1; i <= num * 2; i++) {
            PropertyServiceDependency.addDataDependency(communicationNodes.get(i), taskNodes.get(i), input, graph);
            PropertyServiceDependency.addDataDependency(taskNodes.get(i), communicationNodes.get(i+2), output, graph);
        }

        PropertyServiceDependency.addDataDependency(communicationNodes.get((num * 2) + 1), taskNodes.get((num * 2) + 1), input, graph);
        PropertyServiceDependency.addDataDependency(communicationNodes.get((num * 2) + 2), taskNodes.get((num * 2) + 1), input, graph);
        PropertyServiceDependency.addDataDependency(taskNodes.get((num * 2) + 1), communicationNodes.get((num * 2) + 3), output, graph);

        PropertyServiceData.makeRoot(communicationNodes.get(0));
        PropertyServiceData.makeLeaf(communicationNodes.get((num * 2) + 3));
        PropertyServiceData.setJsonKey(communicationNodes.get(0), input);
        PropertyServiceData.setJsonKey(communicationNodes.get((num * 2) + 3), output);
        return graph;
    }

    private final String localResourceName = "Enactment Engine (Local Machine)";
    private final String cloudResourceName = "https://vbt81rsfof.execute-api.us-east-1.amazonaws.com/Pass/";

    private EnactmentSpecification setupSpecification(EnactmentGraph eGraph, String mappingsPath) {

        // Generate the specification
        final EnactmentGraphProvider eGraphProvider = () -> eGraph;
        final ResourceGraphProvider rGraphProv = new ResourceGraphProviderFile(mappingsPath);
        final SpecificationProviderFile specProv = new SpecificationProviderFile(eGraphProvider, rGraphProv, mappingsPath);
        final EnactmentSpecification specification = specProv.getSpecification();

        // Set up resource instances and latencies
        Resource local = specification.getResourceGraph().getVertex(localResourceName);
        PropertyServiceScheduler.setLatencyLocal(local, 65.0);
        PropertyServiceScheduler.setLatencyGlobal(local, 0.0);
        PropertyServiceScheduler.setInstances(local, 1);
        Resource noop = specification.getResourceGraph().getVertex(cloudResourceName);
        PropertyServiceScheduler.setLatencyLocal(noop, 105.0);
        PropertyServiceScheduler.setLatencyGlobal(noop, 100.0);
        PropertyServiceScheduler.setInstances(noop, 1000);

        // Set up function durations
        MappingsConcurrent mappings = specification.getMappings();
        mappings.mappingStream().forEach((map) -> {
            if(map.getId().contains(localResourceName)) {
                // Docker
                PropertyServiceScheduler.setDuration(map, 5.0);
            } else {
                // Cloud
                PropertyServiceScheduler.setDuration(map, 5.0);
            }
        });
        //mappings.mappingStream().forEach((map) -> PropertyServiceScheduler.setDuration(map, 2000.0));

        return specification;
    }

    public void run() {
        // Get the eGraph and specification (including function durations, task mappings, latencies)
        EnactmentSpecification specification = setupSpecification(getDynamicWf(10), "src/test/resources/wf3.json");

        List<Cut> cuts = new Scheduler().schedule(specification);

        // Cut the workflow at the given position
        /*for(Cut cut: cuts) {
            new Incision().cut(specification, cut.getTopCut(), cut.getBottomCut());
        }*/

        EnactmentGraphViewer.view(specification.getEnactmentGraph());
    }

    public static void main(String[] args) {
        new Runner().run();
    }
}
