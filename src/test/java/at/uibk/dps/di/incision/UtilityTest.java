package at.uibk.dps.di.incision;

import at.uibk.dps.ee.model.graph.EnactmentSpecification;
import nu.xom.ParsingException;
import org.junit.jupiter.api.Test;
import org.xmlunit.builder.DiffBuilder;
import org.xmlunit.diff.DefaultNodeMatcher;
import org.xmlunit.diff.Diff;
import org.xmlunit.diff.ElementSelectors;

import java.io.IOException;
import java.util.Objects;

import static org.junit.jupiter.api.Assertions.*;

/**
 * Test the parsing and generation of {@link EnactmentSpecification}.
 *
 * @author Stefan Pedtascher
 */
public class UtilityTest {

    /**
     * Check the reading and writing of an {@link EnactmentSpecification}.
     *
     * @throws ParsingException on parsing failure.
     * @throws IOException on parsing failure.
     */
    @Test
    void checkSpecificationReaderWriter() throws ParsingException, IOException {
        String specificationFromAFCL = Utility.specFromAFCL(
            Objects.requireNonNull(getClass().getClassLoader().getResource("workflow.yaml")).getPath(),
            Objects.requireNonNull(getClass().getClassLoader().getResource("mapping.json")).getPath());

        EnactmentSpecification enactmentSpecificationFromString = Utility.fromStringToEnactmentSpecification(specificationFromAFCL);

        String enactmentSpecificationToString = Utility.fromEnactmentSpecificationToString(enactmentSpecificationFromString);

        Diff difference = DiffBuilder.compare(specificationFromAFCL)
            .withTest(enactmentSpecificationToString)
            .withNodeMatcher(new DefaultNodeMatcher(ElementSelectors.byNameAndAllAttributes))
            .checkForSimilar()
            .build();

        assertFalse(difference.hasDifferences());
    }
}
