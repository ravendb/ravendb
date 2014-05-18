package net.ravendb.client.connection.profiling;

import java.util.UUID;

import net.ravendb.abstractions.closure.Function1;


public class ProfilingContext {
  private final ConcurrentLruSet<ProfilingInformation> leastRecentlyUsedCache = new ConcurrentLruSet<>(NUMBER_OF_SESSIONS_TO_TRACK, null);

  private static final int NUMBER_OF_SESSIONS_TO_TRACK = 128;

  /**
   * Register the action as associated with the sender
   */
  public void recordAction(Object sender, RequestResultArgs requestResultArgs) {

    if (sender instanceof IHoldProfilingInformation) {
      IHoldProfilingInformation profilingInformationHolder = (IHoldProfilingInformation) sender;
      profilingInformationHolder.getProfilingInformation().getRequests().add(requestResultArgs);
      leastRecentlyUsedCache.push(profilingInformationHolder.getProfilingInformation());
    }

  }

  /**
   * Try to get a session matching the specified id.
   * @param id
   * @return
   */
  public ProfilingInformation tryGet(final UUID id) {
    return leastRecentlyUsedCache.firstOrDefault(new Function1<ProfilingInformation, Boolean>() {

      @Override
      public Boolean apply(ProfilingInformation input) {
        return input.getId().equals(id);
      }
    });
  }
}
