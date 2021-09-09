package at.uibk.dps.di.incision;

import at.uibk.dps.ee.io.afcl.AfclReader;
import at.uibk.dps.ee.io.resources.ResourceGraphProviderFile;
import at.uibk.dps.ee.io.spec.SpecificationProviderFile;
import at.uibk.dps.ee.model.graph.*;
import net.sf.opendse.io.SpecificationReader;
import net.sf.opendse.io.SpecificationWriter;
import net.sf.opendse.model.Mappings;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Specification;
import net.sf.opendse.model.Task;
import nu.xom.ParsingException;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.StandardCharsets;

/**
 * Class containing several utility functions and constants for the distributed engine.
 *
 * @author Stefan Pedratscher
 */
public class Utils {

    /**
     * Represents the location of the distributed engine.
     */
    public static final String DISTRIBUTED_ENGINE_AWS_US_EAST_1 = "https://1m3c0y7o0d.execute-api.us-east-1.amazonaws.com/default/enactment-engine";

    /**
     * Represents the identifier for the distributed engine node.
     */
    public static final String DISTRIBUTED_ENGINE_TYPE_ID = "dEE";

    /**
     * Represents the name of the enactment engine.
     */
    public static final String ENGINE = "Enactment Engine (Local Machine)";

    /**
     * Represents the specification key sent to the distributed enactment engine.
     */
    public static final String SPECIFICATION = "specification";

    /**
     * Represents the configuration key sent to the distributed enactment engine.
     */
    public static final String CONFIGURATION = "configuration";

    /**
     * Represents the default configuration for the distributed engine.
     */
    public static final String DISTRIBUTED_ENGINE_CONFIGURATION = "<configuration>\n"
        + "  <module class=\"at.uibk.dps.ee.control.modules.EnactmentVerticleModule\"/>\n"
        + "  <module class=\"at.uibk.dps.ee.io.modules.InputReaderFileModule\">\n"
        + "    <property name=\"filePath\">./inputData/inputSingleAtomic.json</property>\n"
        + "  </module>\n" + "  <module class=\"at.uibk.dps.ee.io.modules.LoggingModule\">\n"
        + "    <property name=\"pathToConfigFile\">./logging/config/logback.xml</property>\n"
        + "  </module>\n" + "  <module class=\"at.uibk.dps.ee.io.modules.OutputPrinterModule\"/>\n"
        + "  <module class=\"at.uibk.dps.ee.io.modules.SpecificationInputModule\">\n"
        + "    <property name=\"filePathAfcl\">./demoWfs/singleAtomic.yaml</property>\n"
        + "    <property name=\"filePathMappingFile\">./typeMappings/singleAtomic.json</property>\n"
        + "  </module>\n"
        + "  <module class=\"at.uibk.dps.ee.visualization.modules.EnactmentViewerModule\">\n"
        + "    <property name=\"closeOnTerminate\">false</property>\n"
        + "    <property name=\"updatePeriodMs\">100</property>\n" + "  </module>\n"
        + "  <module class=\"at.uibk.dps.sc.core.modules.SchedulerModule\">\n"
        + "    <property name=\"schedulingMode\">SingleOption</property>\n"
        + "    <property name=\"mappingsToPick\">1</property>\n"
        + "    <property name=\"sizeThresholdKb\">10</property>\n" + "  </module>\n"
        + "</configuration>";

    /**
     * Transforms a string to an {@link EnactmentSpecification}.
     *
     * @param specification the string representing the specification.
     *
     * @return the parsed {@link EnactmentSpecification}.
     *
     * @throws ParsingException on parsing failure.
     * @throws IOException on io failure.
     */
    public static EnactmentSpecification fromStringToEnactmentSpecification(String specification) throws ParsingException, IOException {
        final nu.xom.Builder parser = new nu.xom.Builder();
        final nu.xom.Document doc = parser.build(specification, null);
        final nu.xom.Element eSpec = doc.getRootElement();
        final SpecificationReader reader = new SpecificationReader();
        final Specification spec = reader.toSpecification(eSpec);
        final EnactmentGraph eGraph = new EnactmentGraph(spec.getApplication());
        final ResourceGraph rGraph = new ResourceGraph(spec.getArchitecture());
        final Mappings<Task, Resource> mappings = spec.getMappings();
        final EnactmentSpecification result = new EnactmentSpecification(eGraph, rGraph, mappings);
        spec.getAttributeNames()
            .forEach(attrName -> result.setAttribute(attrName, spec.getAttribute(attrName)));
        return result;
    }

    /**
     * Transforms an {@link EnactmentSpecification} to a string.
     *
     * @param enactmentSpecification the {@link EnactmentSpecification} to be parsed.
     *
     * @return string representation of the {@link EnactmentSpecification}.
     */
    public static String fromEnactmentSpecificationToString(EnactmentSpecification enactmentSpecification) {
        final SpecificationWriter writer = new SpecificationWriter();
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        writer.write(enactmentSpecification, stream);
        return stream.toString(StandardCharsets.UTF_8);
    }

    /**
     * Get the specification from an AFCL file.
     *
     * @param filePathAfcl path to the AFCL file.
     * @param filePathTypeMappings path to the mapping file
     *
     * @return the string specification.
     */
    public static String specFromAFCL(String filePathAfcl, String filePathTypeMappings){
        final EnactmentGraphProvider eGraphProv = new AfclReader(filePathAfcl);
        final ResourceGraphProvider rGraphProv = new ResourceGraphProviderFile(filePathTypeMappings);
        final SpecificationProviderFile specProv =
            new SpecificationProviderFile(eGraphProv, rGraphProv, filePathTypeMappings);

        final Specification spec = specProv.getSpecification();
        final SpecificationWriter writer = new SpecificationWriter();
        final ByteArrayOutputStream stream = new ByteArrayOutputStream();
        writer.write(spec, stream);
        return stream.toString(StandardCharsets.UTF_8);
    }
}
