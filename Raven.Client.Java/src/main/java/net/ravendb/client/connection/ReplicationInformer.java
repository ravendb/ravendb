package net.ravendb.client.connection;

import java.io.IOException;
import java.net.SocketException;
import java.net.SocketTimeoutException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;

import net.ravendb.abstractions.basic.EventArgs;
import net.ravendb.abstractions.basic.EventHandler;
import net.ravendb.abstractions.basic.EventHelper;
import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.closure.Function1;
import net.ravendb.abstractions.connection.OperationCredentials;
import net.ravendb.abstractions.data.HttpMethods;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.exceptions.HttpOperationException;
import net.ravendb.abstractions.exceptions.ServerClientException;
import net.ravendb.abstractions.extensions.JsonExtensions;
import net.ravendb.abstractions.json.linq.JTokenType;
import net.ravendb.abstractions.replication.ReplicationDestination;
import net.ravendb.abstractions.replication.ReplicationDocument;
import net.ravendb.client.document.DocumentConvention;
import net.ravendb.client.document.FailoverBehavior;
import net.ravendb.client.extensions.MultiDatabase;

import org.apache.commons.lang.ArrayUtils;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

public class ReplicationInformer implements AutoCloseable {

  private static Log log = LogFactory.getLog(ReplicationInformer.class.getCanonicalName());

  private static String RAVEN_REPLICATION_DESTINATIONS = "Raven/Replication/Destinations";
  private static List<OperationMetadata> EMPTY = new ArrayList<>();

  private boolean firstTime = true;
  protected DocumentConvention conventions;

  protected Date lastReplicationUpdate = new Date(0);
  private final Object replicationLock = new Object();
  private List<OperationMetadata> replicationDestinations = new ArrayList<>();

  protected static AtomicInteger readStripingBase = new AtomicInteger(0);

  private List<EventHandler<FailoverStatusChangedEventArgs>> failoverStatusChanged = new ArrayList<>();

  private final Map<String, FailureCounter> failureCounts = new ConcurrentHashMap<>();

  private Thread refreshReplicationInformationTask;

  private ReplicationDestination[] failoverServers;

  public void setFailoverServers(ReplicationDestination[] failoverServers) {
    this.failoverServers = failoverServers;
  }

  public ReplicationDestination[] getFailoverServers() {
    return failoverServers;
  }

  public ReplicationInformer(DocumentConvention conventions) {
    this.conventions = conventions;
  }

  public List<OperationMetadata> getReplicationDestinations() {
    return this.replicationDestinations;
  }

  public List<OperationMetadata> getReplicationDestinationsUrls() {
    if (FailoverBehavior.FAIL_IMMEDIATELY.equals(this.conventions.getFailoverBehavior())) {
      return EMPTY;
    }
    List<OperationMetadata> result = new ArrayList<>();
    for (OperationMetadata opMeta : this.replicationDestinations) {
      result.add(new OperationMetadata(opMeta));
    }
    return result;
  }

  public void updateReplicationInformationIfNeeded(final ServerClient serverClient) {
    if (conventions.getFailoverBehavior().contains(FailoverBehavior.FAIL_IMMEDIATELY)) {
      return;//new CompletedFuture<>();
    }

    if (DateUtils.addMinutes(lastReplicationUpdate, 5).after(new Date())) {
      return;//new CompletedFuture<>();
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

      Thread taskCopy = refreshReplicationInformationTask;

      if (taskCopy != null) {
        return; //taskCopy;
      }

      refreshReplicationInformationTask = new Thread( new Runnable() {

        @Override
        public void run() {
          try {
            refreshReplicationInformation(serverClient);
            refreshReplicationInformationTask = null;
          } catch (Exception e) {
            log.error("Failed to refresh replication information", e);
          }
        }
      } );

      refreshReplicationInformationTask.start();
    }

  }

  public int getReadStripingBase() {
    return readStripingBase.incrementAndGet();
  }

  public <T> T executeWithReplication(HttpMethods method, String primaryUrl, OperationCredentials primaryCredentials, int currentRequest,
    int currentReadStripingBase, Function1<OperationMetadata, T> operation) throws ServerClientException {

    Reference<T> resultHolder = new Reference<>();
    Reference<Boolean> timeoutThrown = new Reference<>();
    timeoutThrown.value = Boolean.FALSE;

    List<OperationMetadata> localReplicationDestinations = getReplicationDestinationsUrls(); // thread safe copy
    OperationMetadata primaryOperation = new OperationMetadata(primaryUrl, primaryCredentials);

    boolean shouldReadFromAllServers = conventions.getFailoverBehavior().contains(
      FailoverBehavior.READ_FROM_ALL_SERVERS);
    if (shouldReadFromAllServers && HttpMethods.GET.equals(method)) {
      int replicationIndex = currentReadStripingBase % (localReplicationDestinations.size() + 1);
      // if replicationIndex == destinations count, then we want to use the master
      // if replicationIndex < 0, then we were explicitly instructed to use the master
      if (replicationIndex < localReplicationDestinations.size() && replicationIndex >= 0) {
        // if it is failing, ignore that, and move to the master or any of the replicas
        if (shouldExecuteUsing(localReplicationDestinations.get(replicationIndex).getUrl(), currentRequest, method, false)) {
          if (tryOperation(operation, localReplicationDestinations.get(replicationIndex), primaryOperation, true, resultHolder,
            timeoutThrown)) return resultHolder.value;
        }
      }
    }

    if (shouldExecuteUsing(primaryOperation.getUrl(), currentRequest, method, true)) {

      if (tryOperation(operation, primaryOperation, null, !timeoutThrown.value && localReplicationDestinations.size() > 0, resultHolder,
        timeoutThrown)) {
        return resultHolder.value;
      }
      if (!timeoutThrown.value && isFirstFailure(primaryOperation.getUrl())
        && tryOperation(operation, primaryOperation, null, localReplicationDestinations.size() > 0, resultHolder, timeoutThrown)) {
        return resultHolder.value;
      }
      incrementFailureCount(primaryOperation.getUrl());
    }

    for (int i = 0; i < localReplicationDestinations.size(); i++) {
      OperationMetadata replicationDestination = localReplicationDestinations.get(i);
      if (!shouldExecuteUsing(replicationDestination.getUrl(), currentRequest, method, false)) {
        continue;
      }
      if (tryOperation(operation, replicationDestination, primaryOperation, !timeoutThrown.value, resultHolder, timeoutThrown)) {
        return resultHolder.value;
      }
      if (!timeoutThrown.value
        && isFirstFailure(replicationDestination.getUrl())
        && tryOperation(operation, replicationDestination, primaryOperation, localReplicationDestinations.size() > i + 1, resultHolder,
          timeoutThrown)) {
        return resultHolder.value;
      }
      incrementFailureCount(replicationDestination.getUrl());

    }
    // this should not be thrown, but since I know the value of should...
    throw new IllegalStateException("Attempted to connect to master and all replicas have failed, giving up. There is a high probability of a network problem preventing access to all the replicas. Failed to get in touch with any of the " + (1 + localReplicationDestinations.size()) + " Raven instances.");
  }

  protected <T> boolean tryOperation(Function1<OperationMetadata, T> operation, OperationMetadata operationMetadata, OperationMetadata primaryOperationMetadata, boolean avoidThrowing,
    Reference<T> result, Reference<Boolean> wasTimeout) {
    try {
      boolean tryWithPrimaryCredentials = isFirstFailure(operationMetadata.getUrl()) && primaryOperationMetadata != null;

      result.value = operation.apply(tryWithPrimaryCredentials ? new OperationMetadata(operationMetadata.getUrl(), primaryOperationMetadata.getCredentials()) : operationMetadata);
      resetFailureCount(operationMetadata.getUrl());
      wasTimeout.value = Boolean.FALSE;
      return true;
    } catch (Exception e) {
      /* TODO
       * var webException = e as WebException;
                if (tryWithPrimaryCredentials && operationMetadata.Credentials.HasCredentials() && webException != null)
                {
                    IncrementFailureCount(operationMetadata.Url);

                    var response = webException.Response as HttpWebResponse;
                    if (response != null && response.StatusCode == HttpStatusCode.Unauthorized)
                    {
                        return TryOperation(operation, operationMetadata, primaryOperationMetadata, avoidThrowing, out result, out wasTimeout);
                    }
                }
       */
      if (avoidThrowing == false) {
        throw e;
      }
      result.value = null;

      if (isServerDown(e, wasTimeout)) {
        return false;
      }
      throw e;
    }
  }

  public boolean isHttpStatus(Exception e, int... httpStatusCode) {
    if (e instanceof HttpOperationException) {
      HttpOperationException hoe = (HttpOperationException) e;
      if (ArrayUtils.contains(httpStatusCode, hoe.getStatusCode())) {
        return true;
      }
    }
    return false;
  }

  public boolean isServerDown(Exception e, Reference<Boolean> timeout) {
    timeout.value = Boolean.FALSE;
    if (e instanceof SocketTimeoutException) {
      timeout.value = Boolean.TRUE;
      return true;
    }
    if (e instanceof SocketException) {
      return true;
    }
    return false;
  }

  public void dispose() throws InterruptedException {
    Thread replicationInformationTaskCopy = refreshReplicationInformationTask;
    if (replicationInformationTaskCopy != null) {
      replicationInformationTaskCopy.join();
    }
  }


  public AtomicLong getFailureCount(String operationUrl) {
    return getHolder(operationUrl).getValue();
  }

  public Date getFailureLastCheck(String operationUrl) {
    return getHolder(operationUrl).getLastCheck();
  }

  public boolean shouldExecuteUsing(String operationUrl, int currentRequest, HttpMethods method, boolean primary) {
    if (primary == false) {
      assertValidOperation(method);
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

  protected void assertValidOperation(HttpMethods method) {
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
    throw new IllegalStateException("Could not replicate " + method
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
    if (current == 1) { // first failure
      EventHelper.invoke(failoverStatusChanged, this, new FailoverStatusChangedEventArgs(operationUrl, true));
    }
  }

  private static boolean isInvalidDestinationsDocument(JsonDocument document) {
    return document == null || document.getDataAsJson().containsKey("Destinations") == false
      || document.getDataAsJson().get("Destinations") == null
      || JTokenType.NULL.equals(document.getDataAsJson().get("Destinations").getType());
  }

  public void refreshReplicationInformation(ServerClient commands) {
    synchronized (this) {
      String serverHash = ServerHash.getServerHash(commands.getUrl());

      JsonDocument document;
      try {
        //TODO: add credentials
        document = commands.directGet(new OperationMetadata(commands.getUrl()), RAVEN_REPLICATION_DESTINATIONS);
        failureCounts.put(commands.getUrl(), new FailureCounter()); // we just hit the master, so we can reset its failure count
      } catch (Exception e) {
        log.error("Could not contact master for new replication information", e);
        document = ReplicationInformerLocalCache.tryLoadReplicationInformationFromLocalCache(serverHash);
      }
      if (document == null) {
        lastReplicationUpdate = new Date(); // checked and not found
        return;
      }

      ReplicationInformerLocalCache.trySavingReplicationInformationToLocalCache(serverHash, document);
      updateReplicationInformationFromDocument(document);
      lastReplicationUpdate = new Date();
    }
  }

  private void updateReplicationInformationFromDocument(JsonDocument document) {
    ReplicationDocument replicationDocument = null;
    try {
      replicationDocument = JsonExtensions.createDefaultJsonSerializer().readValue(document.getDataAsJson().toString(),
        ReplicationDocument.class);
    } catch (IOException e) {
      log.error("Mapping Exception", e);
      return;
    }
    List<OperationMetadata> replicationDestinations = new ArrayList<>();
    for (ReplicationDestination x : replicationDocument.getDestinations()) {
      String url = StringUtils.isEmpty(x.getClientVisibleUrl()) ? x.getUrl() : x.getClientVisibleUrl();
      if (StringUtils.isEmpty(url) || Boolean.TRUE.equals(x.getDisabled()) || Boolean.TRUE.equals(x.getIgnoredClient())) {
        return;
      }
      if (StringUtils.isEmpty(x.getDatabase())) {
        replicationDestinations.add(new OperationMetadata(url,new OperationCredentials(x.getApiKey())));
        return;
      }
      replicationDestinations.add(new OperationMetadata(MultiDatabase.getRootDatabaseUrl(url) + "/databases/"
        + x.getDatabase(), new OperationCredentials(x.getApiKey())));
    }
    for (OperationMetadata replicationDestination : replicationDestinations) {
      if (!failureCounts.containsKey(replicationDestination.getUrl())) {
        failureCounts.put(replicationDestination.getUrl(), new FailureCounter());
      }
    }
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

  @Override
  public void close() throws Exception {
    Thread informationTask = refreshReplicationInformationTask;
    if (informationTask != null) {
      informationTask.join();
    }
  }

}
