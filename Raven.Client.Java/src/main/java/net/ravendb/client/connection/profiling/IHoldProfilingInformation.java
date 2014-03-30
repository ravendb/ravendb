package net.ravendb.client.connection.profiling;

/**
 * Interface for getting profiling information about the actions of the system
 * within a given context, usually the context is database commands or async database commands
 */
public interface IHoldProfilingInformation {
  /**
   * The profiling information
   */
  public ProfilingInformation getProfilingInformation();

  /**
   * Is expect 100 continue?
   * @return
   */
  public boolean isExpect100Continue();

}
