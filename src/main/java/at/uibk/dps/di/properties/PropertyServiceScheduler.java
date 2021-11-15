package at.uibk.dps.di.properties;

import net.sf.opendse.model.Mapping;
import net.sf.opendse.model.Resource;
import net.sf.opendse.model.Task;
import net.sf.opendse.model.properties.AbstractPropertyService;

/**
 * Static container with methods offering convenient access to the attributes of
 * timing properties.
 *
 * @author Stefan Pedratscher
 */
public class PropertyServiceScheduler extends AbstractPropertyService {

  private static final String propNameDuration = Property.Duration.name();
  private static final String propNameInstances = Property.Instances.name();
  private static final String propNameLatencyGlobal = Property.LatencyGlobal.name();
  private static final String propNameLatencyLocal = Property.LatencyLocal.name();

  /**
   * No constructor.
   */
  private PropertyServiceScheduler() {}

  /**
   * Defines the shared properties
   *
   * @author Fedor Smirnov
   */
  protected enum Property {
    /**
     * The duration of the function on the mapping
     */
    Duration,
    /**
     * The number of available instances of the resource
     */
    Instances,
    /**
     * The latency to the specified resource
     */
    LatencyGlobal,
    /**
     * Tha latency to a function in the same region / on the same device
     */
    LatencyLocal
  }

  /**
   * Sets the duration of a function resource mapping pair
   *
   * @param mapping the given mapping
   * @param duration the duration to set
   */
  public static void setDuration(final Mapping<Task, Resource> mapping, final double duration) {
    mapping.setAttribute(propNameDuration, duration);
  }

  /**
   * Get the duration of a function resource mapping pair
   *
   * @param mapping the given mapping
   */
  public static double getDuration(final Mapping<Task, Resource> mapping) {
    return mapping.getAttribute(propNameDuration);
  }

  /**
   * Sets the instances of the given resource
   *
   * @param res the given resource
   * @param instances the instances to set
   */
  public static void setInstances(final Resource res, final int instances) {
    res.setAttribute(propNameInstances, instances);
  }

  /**
   * Sets the global latency of the given resource
   *
   * @param res the given resource
   * @param latencyGlobal the global latency to set
   */
  public static void setLatencyGlobal(final Resource res, final double latencyGlobal) {
    res.setAttribute(propNameLatencyGlobal, latencyGlobal);
  }

  /**
   * Sets the local latency of the given resource
   *
   * @param res the given resource
   * @param latencyLocal the local latency to set
   */
  public static void setLatencyLocal(final Resource res, final double latencyLocal) {
    res.setAttribute(propNameLatencyLocal, latencyLocal);
  }

  /**
   * Returns the number of instances of the given resource
   *
   * @param res the given resource
   * @return the number of instances of the given resource
   */
  public static int getInstances(final Resource res) {
    return (int) getAttribute(res, propNameInstances);
  }

  /**
   * Returns the global latency of the given resource
   *
   * @param res the given resource
   * @return the global latency of the given resource
   */
  public static double getLatencyGlobal(final Resource res) {
    return (double) getAttribute(res, propNameLatencyGlobal);
  }

  /**
   * Returns the local latency of the given resource
   *
   * @param res the given resource
   * @return the local latency of the given resource
   */
  public static double getLatencyLocal(final Resource res) {
    return (double) getAttribute(res, propNameLatencyLocal);
  }
}
