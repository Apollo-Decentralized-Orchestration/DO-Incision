package at.uibk.dps.di;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import at.uibk.dps.di.incision.Incision;
import at.uibk.dps.di.incision.Utility;
import at.uibk.dps.di.properties.PropertyServiceScheduler;
import at.uibk.dps.di.scheduler.Cut;
import at.uibk.dps.di.scheduler.Scheduler;
import at.uibk.dps.ee.deploy.run.ImplementationRunBare;
import at.uibk.dps.ee.deploy.spec.SpecFromString;
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
import nu.xom.ParsingException;

public class Runner {

    private final String CONFIG = "<configuration>\n" +
            "  <module class=\"at.uibk.dps.ee.control.modules.EnactmentVerticleModule\">\n" +
            "    <property name=\"pauseOnStart\">false</property>\n" +
            "    <property name=\"deploymentNumber\">8</property>\n" +
            "  </module>\n" +
            "  <module class=\"at.uibk.dps.ee.docker.modules.ContainersModule\">\n" +
            "    <property name=\"dockerManager\">DockerApi</property>\n" +
            "    <property name=\"usedOs\">Windows</property>\n" +
            "  </module>\n" +
            "  <module class=\"at.uibk.dps.ee.io.modules.InputReaderFileModule\">\n" +
            "    <property name=\"filePath\">/home/stefan/Desktop/Apollo/TimingAnalysis/src/test/resources/in.json</property>\n" +
            "  </module>\n" +
            "  <module class=\"at.uibk.dps.ee.io.modules.LoggingModule\">\n" +
            "    <property name=\"pathToConfigFile\">./logging/config/logback.xml</property>\n" +
            "  </module>\n" +
            "  <module class=\"at.uibk.dps.ee.io.modules.OutputPrinterModule\"/>\n" +
            "  <module class=\"at.uibk.dps.ee.io.modules.SpecificationInputModule\">\n" +
            "    <property name=\"filePathAfcl\">/home/stefan/Desktop/Apollo/TimingAnalysis/src/test/resources/wf3.yaml</property>\n" +
            "    <property name=\"filePathMappingFile\">/home/stefan/Desktop/Apollo/TimingAnalysis/src/test/resources/wf3.json</property>\n" +
            "  </module>\n" +
            "  <module class=\"at.uibk.dps.ee.visualization.modules.EnactmentViewerModule\">\n" +
            "    <property name=\"closeOnTerminate\">false</property>\n" +
            "    <property name=\"updatePeriodMs\">100</property>\n" +
            "  </module>\n" +
            "  <module class=\"at.uibk.dps.sc.core.modules.SchedulerModule\">\n" +
            "    <property name=\"schedulingMode\">Random</property>\n" +
            "    <property name=\"mappingsToPick\">1</property>\n" +
            "    <property name=\"sizeThresholdKb\">10</property>\n" +
            "  </module>\n" +
            "</configuration>\n";

    public static EnactmentGraph getDynamicWf(int num, int par){
        final List<Task> communicationNodes = new ArrayList<Task>();
        final List<Task> taskNodes = new ArrayList<Task>();

        String input = "in";
        String output = "out";

        for(int i = 0; i < (par + par * num); i++) {
            communicationNodes.add(new Communication("commNode" + i));
            taskNodes.add(PropertyServiceFunctionUser.createUserTask("taskNode" + i, "Pass"));
        }

        EnactmentGraph graph = new EnactmentGraph();
        PropertyServiceDependency.addDataDependency(communicationNodes.get(0), taskNodes.get(0), input, graph);

        for(int i = 0; i < par; i++) {
            communicationNodes.add(new Communication("commNode" + ((par + par * num) + i)));
            PropertyServiceDependency.addDataDependency(taskNodes.get(0), communicationNodes.get(i + 1), output, graph);
        }

        for(int i = 1; i <= num * par; i++) {
            PropertyServiceDependency.addDataDependency(communicationNodes.get(i), taskNodes.get(i), input, graph);
            PropertyServiceDependency.addDataDependency(taskNodes.get(i), communicationNodes.get(i + par), output, graph);
        }

        int taskNodeIndex = (num * par) + 1;
        int commNode = (num * par) + 1;
        if(par == 1) {
            taskNodeIndex = (num * par);
            commNode = (num * par);
        }

        for(int i = 0; i < par; i++) {
            PropertyServiceDependency.addDataDependency(communicationNodes.get(commNode + i),
                    taskNodes.get(taskNodeIndex), input, graph);
        }


        PropertyServiceDependency.addDataDependency(taskNodes.get(taskNodeIndex), communicationNodes.get((num * par) + par - 1 + par), output, graph);

        PropertyServiceData.makeRoot(communicationNodes.get(0));
        PropertyServiceData.setJsonKey(communicationNodes.get(0), input);

        PropertyServiceData.makeLeaf(communicationNodes.get((num * par) + par - 1 + par));
        PropertyServiceData.setJsonKey(communicationNodes.get((num * par) + par - 1 + par), output);
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
        PropertyServiceScheduler.setLatencyLocal(noop, 450.0);
        PropertyServiceScheduler.setLatencyGlobal(noop, 2050.0);
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
        mappings.mappingStream().forEach((map) -> PropertyServiceScheduler.setDuration(map, 1000.0));

        return specification;
    }

    public void run() {
        // Get the eGraph and specification (including function durations, task mappings, latencies)
        EnactmentSpecification specification = setupSpecification(getDynamicWf(10, 2), "src/test/resources/wf3.json");

        List<Cut> cuts = new Scheduler().schedule(specification);

        // Cut the workflow at the given position
        for(Cut cut: cuts) {
            new Incision().cut(specification, cut.getTopCut(), cut.getBottomCut());
        }

        String specificationAdapted = Utility.fromEnactmentSpecificationToString(specification);
        //EnactmentGraphViewer.view(specification.getEnactmentGraph());

        String input = new String(new char[500000]).replace("\0", "*");
        String in = "{ 'in': '" + input + "' }";

        // Run the workflow
        double start = System.currentTimeMillis();
        //new ImplementationRunBare().implement(in, specificationAdapted, CONFIG);
        double end = System.currentTimeMillis();
        System.out.println("Duration: " + (end - start));

    }

    public static void main(String[] args) {
        new Runner().run();
    }
}
