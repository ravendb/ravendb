package raven.client.connection;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import javax.activity.InvalidActivityException;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;

import raven.abstractions.basic.EventArgs;
import raven.abstractions.basic.EventHandler;
import raven.abstractions.closure.Function1;
import raven.abstractions.data.HttpMethods;
import raven.abstractions.data.JsonDocument;
import raven.abstractions.exceptions.ServerClientException;
import raven.abstractions.json.linq.JTokenType;
import raven.client.document.DocumentConvention;
import raven.client.document.FailoverBehavior;

// TODO: finish me
public class ReplicationInformer {

  private static Log log = LogFactory.getLog(ReplicationInformer.class.getCanonicalName());

  private static String RAVEN_REPLICATION_DESTINATIONS = "Raven/Replication/Destinations";
  private static List<String> EMPTY = new ArrayList<>();
  protected static int READ_STRIPING_BASE;

  private boolean firstTime = true;
  protected DocumentConvention conventions;

  protected Date lastReplicationUpdate; // = DateTime.MinValue;
  private final Object replicationLock = new Object();
  private List<ReplicationDestinationData> replicationDestinations = new ArrayList<>();

  protected static AtomicInteger readStripingBase = new AtomicInteger(0);

  //System.Collections.Concurrent.ConcurrentDictionary
  private final Map<String, FailureCounter> failureCounts = new ConcurrentHashMap<String, FailureCounter>();

  public ReplicationInformer(DocumentConvention conventions) {
    this.conventions = conventions;
  }

  public ReplicationInformer() {
  }


  //TODO: public event EventHandler<FailoverStatusChangedEventArgs> FailoverStatusChanged = delegate { };
  //EventHandler<FailoverStatusChangedEventArgs> failoverStatusChanged = new ArrayList<>();

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

  public void updateReplicationInformationIfNeeded(ServerClient serverClient) {
    /* if (conventions.FailoverBehavior == FailoverBehavior.FailImmediately)
       return new CompletedTask();

    if (lastReplicationUpdate.AddMinutes(5) > SystemTime.UtcNow)
       return new CompletedTask();

    lock (replicationLock)
    {
       if (firstTime)
       {
           var serverHash = ServerHash.GetServerHash(serverClient.Url);

           var document = ReplicationInformerLocalCache.TryLoadReplicationInformationFromLocalCache(serverHash);
           if (IsInvalidDestinationsDocument(document) == false)
           {
               UpdateReplicationInformationFromDocument(document);
           }
       }

       firstTime = false;

       if (lastReplicationUpdate.AddMinutes(5) > SystemTime.UtcNow)
           return new CompletedTask();

       var taskCopy = refreshReplicationInformationTask;
       if (taskCopy != null)
           return taskCopy;

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

  //TODO: impl me

  public int getReadStripingBase() {
    return readStripingBase.incrementAndGet();
  }

  public <T> T executeWithReplication(HttpMethods method, String url, int currentRequest, int currentReadStripingBase,
    Function1<String, T> operation) throws ServerClientException {
    return operation.apply(url);
    //TODO: implement me
  }

  public long getFailureCount(String operationUrl) {
    // TODO Auto-generated method stub
    return getHolder(operationUrl).getValue();
  }

  public Date getFailureLastCheck(String operationUrl) {
    return getHolder(operationUrl).getLastCheck();
  }

  public boolean shouldExecuteUsing(String operationUrl, int currentRequest, String method, boolean primary)
    throws InvalidActivityException {
    if (primary == false) {
      assertValidOperation(method);
    }

    FailureCounter failureCounter = getHolder(operationUrl);
    if (failureCounter.getValue() == 0 || failureCounter.isForceCheck()) {
      failureCounter.setLastCheck(new Date());
      return true;
    }

    if (currentRequest % getCheckRepetitionRate(failureCounter.getValue()) == 0) {
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

  protected void assertValidOperation(String method) throws InvalidActivityException {
    if (conventions.getFailoverBehaviorWithoutFlags().contains(FailoverBehavior.ALLOW_READS_FROM_SECONDARIES)) {
      if ("GET".equals(method)) {
        return;
      }
    }
    if (conventions.getFailoverBehaviorWithoutFlags().contains(
      FailoverBehavior.ALLOW_READS_FROM_SECONDARIES_AND_WRITES_TO_SECONDARIES)) {
      return;
    }
    if (conventions.getFailoverBehaviorWithoutFlags().contains(FailoverBehavior.FAIL_IMMEDIATELY)) {
      if (conventions.getFailoverBehaviorWithoutFlags().contains(FailoverBehavior.READ_FROM_ALL_SERVERS)) {
        if ("GET".equals(method)) {
          return;
        }
      }
    }
    throw new InvalidActivityException("Could not replicate " + method
      + " operation to secondary node, failover behavior is: " + conventions.getFailoverBehavior());
  }

  public boolean isFirstFailure(String operationUrl) {
    FailureCounter value = getHolder(operationUrl);
    return value.getValue() == 0;
  }

  public void incrementFailureCount(String operationUrl) {
    FailureCounter value = getHolder(operationUrl);
    value.setForceCheck(false);
    long current = value.getValue();//= Interlocked.Increment(ref value.Value);
    if (current == 1)// first failure
    {
      //      failoverStatusChanged(this, new FailoverStatusChangedEventArgs
      //      {
      //          Url = operationUrl,
      //          Failing = true
      //      });
    }
  }

  private static boolean isInvalidDestinationsDocument(JsonDocument document) {
    return document == null || document.getDataAsJson().containsKey("Destinations") == false
      || document.getDataAsJson().get("Destinations") == null
      || JTokenType.NULL.equals(document.getDataAsJson().get("Destinations").getType());
  }

  public void refreshReplicationInformation(ServerClient commands){
  //TODO: implement me
  }

  private void updateReplicationInformationFromDocument(JsonDocument document) {
    //TODO: implement me
  }

  public void resetFailureCount(String operationUrl)
  {
      FailureCounter value = getHolder(operationUrl);
      //var oldVal = Interlocked.Exchange(ref value.Value, 0);
      long oldVal = value.getValue();
      value.setValue(0);
      value.setLastCheck(new Date());
      value.setForceCheck(false);
      if (oldVal != 0)
      {
        /*  FailoverStatusChanged(this,
              new FailoverStatusChangedEventArgs
              {
                  Url = operationUrl,
                  Failing = false
              });*/
      }
  }


  public static class FailoverStatusChangedEventArgs extends EventArgs {

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
    // TODO Auto-generated method stub

  }

  public void removeFailoverStatusChanged(EventHandler<FailoverStatusChangedEventArgs> event) {
    // TODO Auto-generated method stub

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

    private long value;
    private Date lastCheck;
    private boolean forceCheck;

    public long getValue() {
      return value;
    }

    public void setValue(long value) {
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
