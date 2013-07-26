package raven.client.connection;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import javax.activity.InvalidActivityException;

import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import raven.abstractions.basic.EventArgs;
import raven.abstractions.basic.EventHandler;
import raven.abstractions.basic.EventHelper;
import raven.abstractions.basic.Holder;
import raven.abstractions.closure.Function0;
import raven.abstractions.closure.Function1;
import raven.abstractions.data.HttpMethods;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.exceptions.ServerClientException;
import raven.abstractions.json.linq.JTokenType;
import raven.abstractions.task.TaskFactory;
import raven.client.document.DocumentConvention;
import raven.client.document.FailoverBehavior;

import com.google.common.util.concurrent.FutureCallback;

// TODO: finish me
public class ReplicationInformer {

  private static Log log = LogFactory.getLog(ReplicationInformer.class.getCanonicalName());

  private static String RAVEN_REPLICATION_DESTINATIONS = "Raven/Replication/Destinations";
  private static List<String> EMPTY = new ArrayList<>();

  private boolean firstTime = true;
  protected DocumentConvention conventions;

  protected Date lastReplicationUpdate; // = DateTime.MinValue;
  private final Object replicationLock = new Object();
  private List<ReplicationDestinationData> replicationDestinations = new ArrayList<>();

  protected static AtomicInteger readStripingBase = new AtomicInteger(0);

  private List<EventHandler<FailoverStatusChangedEventArgs>> failoverStatusChanged = new ArrayList<>();

  private final Map<String, FailureCounter> failureCounts = new ConcurrentHashMap<>();

  private Future<Void> refreshReplicationInformationTask;

  public ReplicationInformer(DocumentConvention conventions) {
    this.conventions = conventions;
  }

  public List<ReplicationDestinationData> getReplicationDestinations() {
    return this.replicationDestinations;
  }

  public List<String> getReplicationDestinationsUrls() {
    if (FailoverBehavior.FAIL_IMMEDIATELY.equals(this.conventions.getFailoverBehavior())) {
      return EMPTY;
    }
    List<String> result = new ArrayList<>();
    for (ReplicationDestinationData rdd : this.replicationDestinations) {
      result.add(rdd.getUrl());
    }
    return result;
  }

  //TODO: consider return the Future<Void>
  public void updateReplicationInformationIfNeeded(final ServerClient serverClient) {
    if (conventions.getFailoverBehavior().contains(FailoverBehavior.FAIL_IMMEDIATELY)) {
      return; //new CompletedFuture<>();
    }

    if (DateUtils.addMinutes(lastReplicationUpdate, 5).after(new Date())) {
      return; // new CompletedFuture<>();
    }

    synchronized (replicationLock) {
      if (firstTime) {
        String serverHash = ServerHash.getServerHash(serverClient.getUrl());

        JsonDocument document = ReplicationInformerLocalCache.tryLoadReplicationInformationFromLocalCache(serverHash);
        if (!isInvalidDestinationsDocument(document)) {
          updateReplicationInformationFromDocument(document);
        }
      }

      firstTime = false;

      if (DateUtils.addMinutes(lastReplicationUpdate, 5).after(new Date())) {
        return; //new CompletedFuture<>();
      }

      Future<Void> taskCopy = refreshReplicationInformationTask;

      if (taskCopy != null) {
        return;  //taskCopy;
      }

      TaskFactory.startNew(new Function0<Void>() {

        @Override
        public Void apply() {
          refreshReplicationInformation(serverClient);
          return null;
        }

      }, new FutureCallback<Void>() {

        @Override
        public void onFailure(Throwable e) {
          log.error("Failed to refresh replication information", e);
        }

        @Override
        public void onSuccess(Void v) {
          refreshReplicationInformationTask = null;

        }
      });
    }

    /*
           return refreshReplicationInformationTask = Task.Factory.StartNew(() => RefreshReplicationInformation(serverClient))
               .ContinueWith(task =>
               {
                   if (task.Exception != null)
                   {
                       log.ErrorException("Failed to refresh replication information", task.Exception);
                   }
                   refreshReplicationInformationTask = null;
               });
        }*/

  }

  public int getReadStripingBase() {
    return readStripingBase.incrementAndGet();
  }

  public <T> T executeWithReplication(HttpMethods method, String primaryUrl, int currentRequest, int currentReadStripingBase,
    Function1<String, T> operation) throws ServerClientException {

    Holder<T> resultHolder = new Holder<>();
    boolean timeoutThrown = false;

    List<String> localReplicationDestinations = getReplicationDestinationsUrls(); // thread safe copy

    boolean shouldReadFromAllServers = conventions.getFailoverBehavior().contains(FailoverBehavior.READ_FROM_ALL_SERVERS);
    if (shouldReadFromAllServers && HttpMethods.GET.equals(method))
    {
        int replicationIndex = currentReadStripingBase % (localReplicationDestinations.size() + 1);
        // if replicationIndex == destinations count, then we want to use the master
        // if replicationIndex < 0, then we were explicitly instructed to use the master
        if (replicationIndex < localReplicationDestinations.size() && replicationIndex >= 0) {
            // if it is failing, ignore that, and move to the master or any of the replicas
            if (shouldExecuteUsing(localReplicationDestinations.get(replicationIndex), currentRequest, method, false)) {
                if (tryOperation(operation, localReplicationDestinations.get(replicationIndex), true, resultHolder, timeoutThrown))
                    return resultHolder.value;
            }
        }
    }

    if (shouldExecuteUsing(primaryUrl, currentRequest, method, true)) {
        if (tryOperation(operation, primaryUrl, !timeoutThrown && localReplicationDestinations.size() > 0, resultHolder, timeoutThrown)) {
            return resultHolder.value;
        }
        if (!timeoutThrown && isFirstFailure(primaryUrl) &&
            tryOperation(operation, primaryUrl, localReplicationDestinations.size() > 0, resultHolder, timeoutThrown)) {
            return resultHolder.value;
        }
        incrementFailureCount(primaryUrl);
    }

    for (int i = 0; i < localReplicationDestinations.size(); i++) {
        String replicationDestination = localReplicationDestinations.get(i);
        if (!shouldExecuteUsing(replicationDestination, currentRequest, method, false)) {
            continue;
        }
        if (tryOperation(operation, replicationDestination, !timeoutThrown, resultHolder, timeoutThrown)) {
            return resultHolder.value;
        }
        if (!timeoutThrown && isFirstFailure(replicationDestination) &&
            tryOperation(operation, replicationDestination, localReplicationDestinations.size() > i + 1, resultHolder,
                         timeoutThrown)) {
            return resultHolder.value;
        }
        incrementFailureCount(replicationDestination);
    }
    // this should not be thrown, but since I know the value of should...
    //TODO: throw new InvalidActivityException("Attempted to connect to master and all replicas have failed, giving up. There is a high probability of a network problem preventing access to all the replicas. Failed to get in touch with any of the " + (1 + localReplicationDestinations.size()) + " Raven instances.");
    return null;
  }

  //TODO: finish me
  protected <T> boolean tryOperation(Function1<String, T> operation, String operationUrl, boolean avoidThrowing, Holder<T> result, boolean wasTimeout)
  {
      try
      {
          result.value = operation.apply(operationUrl);
          resetFailureCount(operationUrl);
          wasTimeout = false;
          return true;
      }
      catch (Exception e)
      {
          if (avoidThrowing == false)
              throw e;
          //result = default(T);

          if (isServerDown(e, wasTimeout))
          {
              return false;
          }
          throw e;
      }
  }

  public boolean isServerDown(Exception e, boolean timeout)
  {
    //TODO: implement me
    return true;
  }

  public AtomicLong getFailureCount(String operationUrl) {
    return getHolder(operationUrl).getValue();
  }

  public Date getFailureLastCheck(String operationUrl) {
    return getHolder(operationUrl).getLastCheck();
  }

  public boolean shouldExecuteUsing(String operationUrl, int currentRequest, HttpMethods method, boolean primary) {
    if (primary == false) {
      try {
        assertValidOperation(method);
      } catch (InvalidActivityException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
    }

    FailureCounter failureCounter = getHolder(operationUrl);
    if (failureCounter.getValue().longValue() == 0 || failureCounter.isForceCheck()) {
      failureCounter.setLastCheck(new Date());
      return true;
    }

    if (currentRequest % getCheckRepetitionRate(failureCounter.getValue().longValue()) == 0) {
      failureCounter.setLastCheck(new Date());
      return true;
    }

    if ((System.currentTimeMillis() - failureCounter.getLastCheck().getTime()) > conventions
      .getMaxFailoverCheckPeriod()) {
      failureCounter.setLastCheck(new Date());
      return true;
    }

    return false;
  }

  private int getCheckRepetitionRate(long value) {
    if (value < 2) return (int) value;
    if (value < 10) return 2;
    if (value < 100) return 10;
    if (value < 1000) return 100;
    if (value < 10000) return 1000;
    if (value < 100000) return 10000;
    return 100000;
  }

  protected void assertValidOperation(HttpMethods method) throws InvalidActivityException {
    if (conventions.getFailoverBehaviorWithoutFlags().contains(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES)) {
      if (HttpMethods.GET.equals(method)) {
        return;
      }
    }
    if (conventions.getFailoverBehaviorWithoutFlags().contains(
      FailoverBehavior.ALLOW_READS_FROM_SECONDARIES_AND_WRITES_TO_SECONDARIES)) {
      return;
    }
    if (conventions.getFailoverBehaviorWithoutFlags().contains(FailoverBehavior.FAIL_IMMEDIATELY)) {
      if (conventions.getFailoverBehaviorWithoutFlags().contains(FailoverBehavior.READ_FROM_ALL_SERVERS)) {
        if (HttpMethods.GET.equals(method)) {
          return;
        }
      }
    }
    throw new InvalidActivityException("Could not replicate " + method
      + " operation to secondary node, failover behavior is: " + conventions.getFailoverBehavior());
  }

  public boolean isFirstFailure(String operationUrl) {
    FailureCounter value = getHolder(operationUrl);
    return value.getValue().longValue() == 0;
  }

  public void incrementFailureCount(String operationUrl) {
    FailureCounter value = getHolder(operationUrl);
    value.setForceCheck(false);
    long current = value.getValue().incrementAndGet();
    if (current == 1)// first failure
    {
      EventHelper.invoke(failoverStatusChanged, this, new FailoverStatusChangedEventArgs(operationUrl, true));

    }
  }

  private static boolean isInvalidDestinationsDocument(JsonDocument document) {
    return document == null || document.getDataAsJson().containsKey("Destinations") == false
      || document.getDataAsJson().get("Destinations") == null
      || JTokenType.NULL.equals(document.getDataAsJson().get("Destinations").getType());
  }

  public void refreshReplicationInformation(ServerClient commands) {
    //TODO: implement me
  }

  private void updateReplicationInformationFromDocument(JsonDocument document) {
    //TODO: implement me
  }

  public void resetFailureCount(String operationUrl) {
    FailureCounter value = getHolder(operationUrl);
    long oldVal = value.getValue().getAndSet(0);
    value.setLastCheck(new Date());
    value.setForceCheck(false);
    if (oldVal != 0) {
      EventHelper.invoke(failoverStatusChanged, this, new FailoverStatusChangedEventArgs(operationUrl, false));
    }
  }

  public static class FailoverStatusChangedEventArgs extends EventArgs {

    public FailoverStatusChangedEventArgs() {
    }

    public FailoverStatusChangedEventArgs(String url, Boolean failing) {
      super();
      this.failing = failing;
      this.url = url;
    }

    private Boolean failing;
    private String url;

    /**
     * @return the failing
     */
    public Boolean getFailing() {
      return failing;
    }

    /**
     * @param failing the failing to set
     */
    public void setFailing(Boolean failing) {
      this.failing = failing;
    }

    /**
     * @return the url
     */
    public String getUrl() {
      return url;
    }

    /**
     * @param url the url to set
     */
    public void setUrl(String url) {
      this.url = url;
    }

  }

  public void addFailoverStatusChanged(EventHandler<FailoverStatusChangedEventArgs> event) {
    failoverStatusChanged.add(event);
  }

  public void removeFailoverStatusChanged(EventHandler<FailoverStatusChangedEventArgs> event) {
    failoverStatusChanged.remove(event);
  }

  public void forceCheck(String primaryUrl, boolean shouldForceCheck) {
    FailureCounter failureCounter = getHolder(primaryUrl);
    failureCounter.setForceCheck(shouldForceCheck);
  }

  protected FailureCounter getHolder(String operationUrl) {
    if (!failureCounts.containsKey(operationUrl)) {
      failureCounts.put(operationUrl, new FailureCounter());
    }
    return failureCounts.get(operationUrl);

  }

  public class FailureCounter {

    private AtomicLong value = new AtomicLong();
    private Date lastCheck;
    private boolean forceCheck;

    public AtomicLong getValue() {
      return value;
    }

    public void setValue(AtomicLong value) {
      this.value = value;
    }

    public Date getLastCheck() {
      return lastCheck;
    }

    public void setLastCheck(Date lastCheck) {
      this.lastCheck = lastCheck;
    }

    public boolean isForceCheck() {
      return forceCheck;
    }

    public void setForceCheck(boolean forceCheck) {
      this.forceCheck = forceCheck;
    }

    public FailureCounter() {
      this.lastCheck = new Date();
    }
  }

  public class ReplicationDestinationData {

    private String url;

    public String getUrl() {
      return url;
    }

    public void setUrl(String url) {
      this.url = url;
    }
  }

}
