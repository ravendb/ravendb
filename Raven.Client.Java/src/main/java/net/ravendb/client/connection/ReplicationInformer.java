package net.ravendb.client.connection;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.ravendb.abstractions.basic.EventArgs;
import net.ravendb.abstractions.connection.OperationCredentials;
import net.ravendb.abstractions.data.JsonDocument;
import net.ravendb.abstractions.extensions.JsonExtensions;
import net.ravendb.abstractions.replication.ReplicationDestination;
import net.ravendb.abstractions.replication.ReplicationDocument;
import net.ravendb.client.document.Convention;
import net.ravendb.client.document.FailoverBehavior;
import net.ravendb.client.extensions.MultiDatabase;

import org.apache.commons.lang.StringUtils;
import org.apache.commons.lang.time.DateUtils;

public class ReplicationInformer extends ReplicationInformerBase<ServerClient> implements IDocumentStoreReplicationInformer {

  private static String RAVEN_REPLICATION_DESTINATIONS = "Raven/Replication/Destinations";

  private ReplicationDestination[] failoverServers;

  public void setFailoverServers(ReplicationDestination[] failoverServers) {
    this.failoverServers = failoverServers;
  }

  public ReplicationDestination[] getFailoverServers() {
    return failoverServers;
  }

  public ReplicationInformer(Convention conventions) {
    super(conventions);
  }

  @Override
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

  @Override
  public void refreshReplicationInformation(ServerClient commands) {
    synchronized (this) {
      String serverHash = ServerHash.getServerHash(commands.getUrl());

      JsonDocument document;
      try {
        document = commands.directGet(new OperationMetadata(commands.getUrl(), commands.getPrimaryCredentials()), RAVEN_REPLICATION_DESTINATIONS);
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


  @Override
  public void close() {
    Thread informationTask = refreshReplicationInformationTask;
    if (informationTask != null) {
      try {
        informationTask.join();
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }

}
