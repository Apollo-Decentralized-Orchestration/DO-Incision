package at.uibk.dps.di.scheduler;

import net.sf.opendse.model.Task;
import org.junit.jupiter.api.Test;
import java.util.HashSet;
import java.util.Set;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class CutTest {

    @Test
    public void testCut() {

        Set<Task> topCut = new HashSet<>();
        topCut.add(new Task("t1"));

        Set<Task> bottomCut = new HashSet<>();
        bottomCut.add(new Task("b1"));

        Cut cut = new Cut(topCut, bottomCut);

        assertEquals(topCut, cut.getTopCut());
        assertEquals(bottomCut, cut.getBottomCut());
    }
}
