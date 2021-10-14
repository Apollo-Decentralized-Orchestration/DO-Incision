package at.uibk.dps.di.scheduler;

import net.sf.opendse.model.Task;

import java.util.Set;

/**
 * Class representing a workflow cut, consisting of a top and bottom cut.
 *
 * @author Stefan Pedratscher
 */
public class Cut {

    /**
     * Represents the top cut.
     */
    private Set<Task> topCut;

    /**
     * Represents the bottom cut
     */
    private Set<Task> bottomCut;

    /**
     * Default constructor.
     *
     * @param topCut the top cut.
     * @param bottomCut the bottom cut.
     */
    public Cut(Set<Task> topCut, Set<Task> bottomCut) {
        this.topCut = topCut;
        this.bottomCut = bottomCut;
    }

    /**
     * Getter and Setter
     */

    public Set<Task> getTopCut() {
        return topCut;
    }

    public void setTopCut(Set<Task> topCut) {
        this.topCut = topCut;
    }

    public Set<Task> getBottomCut() {
        return bottomCut;
    }

    public void setBottomCut(Set<Task> bottomCut) {
        this.bottomCut = bottomCut;
    }
}
