package at.uibk.dps.di.scheduler;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class ResourceTest {

    @Test
    public void testResource() {
        Resource resource = new Resource("type", 1, 100, 400);

        assertEquals(100.0, resource.earliestStartTime(0, true));
        assertEquals(500.0, resource.earliestStartTime(0, false));

        assertEquals(1100.0, resource.setResource(0, 1000.0, true));

        assertEquals(1200.0, resource.earliestStartTime(0, true));
        assertEquals(1600.0, resource.earliestStartTime(0, false));
    }
}
