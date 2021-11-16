package at.uibk.dps.di.properties;

import at.uibk.dps.ee.model.properties.PropertyServiceMapping;
import at.uibk.dps.ee.model.properties.PropertyServiceResource;
import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class PropertyServiceSchedulerTest {
    @Test
    public void testMappingDuration() {
        Mapping<Task, Resource> mapping = PropertyServiceMapping.createMapping(new Task("task"),
            new Resource("res"), PropertyServiceMapping.EnactmentMode.Local, "impl1");

        double duration = 12;
        PropertyServiceScheduler.setDuration(mapping, duration);
        assertEquals(PropertyServiceScheduler.getDuration(mapping), duration);
    }

    @Test
    public void testLatency() {
        String id = "resId";
        double latencyGlobal = 500.0;
        double latencyLocal = 200.0;

        Resource result = PropertyServiceResource.createResource(id);
        assertThrows(IllegalArgumentException.class, () -> PropertyServiceScheduler.getLatencyGlobal(result));
        assertThrows(IllegalArgumentException.class, () -> PropertyServiceScheduler.getLatencyLocal(result));
        PropertyServiceScheduler.setLatencyGlobal(result, latencyGlobal);
        PropertyServiceScheduler.setLatencyLocal(result, latencyLocal);
        assertEquals(latencyGlobal, PropertyServiceScheduler.getLatencyGlobal(result));
        assertEquals(latencyLocal, PropertyServiceScheduler.getLatencyLocal(result));
    }

    @Test
    public void testInstances() {
        String id = "resId";
        Resource result = PropertyServiceResource.createResource(id);
        assertThrows(IllegalArgumentException.class, () -> PropertyServiceScheduler.getInstances(result));
        PropertyServiceScheduler.setInstances(result, 5);
        assertEquals(5, PropertyServiceScheduler.getInstances(result));
    }
}
