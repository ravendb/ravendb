package net.ravendb.abstractions.replication;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.ravendb.abstractions.data.Etag;


public class ReplicationStatistics {

  private String self;
  private Etag mostRecentDocumentEtag;
  private Etag mostRecentAttachmentEtag;
  private List<DestinationStats> stats;

  public ReplicationStatistics() {
    this.stats = new ArrayList<>();
  }

  public String getSelf() {
    return self;
  }

  public void setSelf(String self) {
    this.self = self;
  }

  public Etag getMostRecentDocumentEtag() {
    return mostRecentDocumentEtag;
  }

  public void setMostRecentDocumentEtag(Etag mostRecentDocumentEtag) {
    this.mostRecentDocumentEtag = mostRecentDocumentEtag;
  }

  public Etag getMostRecentAttachmentEtag() {
    return mostRecentAttachmentEtag;
  }

  public void setMostRecentAttachmentEtag(Etag mostRecentAttachmentEtag) {
    this.mostRecentAttachmentEtag = mostRecentAttachmentEtag;
  }

  public List<DestinationStats> getStats() {
    return stats;
  }

  public void setStats(List<DestinationStats> stats) {
    this.stats = stats;
  }

  public class DestinationStats {

    private int failureCountInternal = 0;
    private String url;
    private Date lastHeartbeatReceived;
    private Etag lastEtagCheckedForReplication;
    private Etag lastReplicatedEtag;
    private Date lastReplicatedLastModified;
    private Date lastSuccessTimestamp;
    private Date lastFailureTimestamp;

    public int getFailureCountInternal() {
      return failureCountInternal;
    }

    public void setFailureCountInternal(int failureCountInternal) {
      this.failureCountInternal = failureCountInternal;
    }

    public String getUrl() {
      return url;
    }

    public void setUrl(String url) {
      this.url = url;
    }

    public Date getLastHeartbeatReceived() {
      return lastHeartbeatReceived;
    }

    public void setLastHeartbeatReceived(Date lastHeartbeatReceived) {
      this.lastHeartbeatReceived = lastHeartbeatReceived;
    }

    public Etag getLastEtagCheckedForReplication() {
      return lastEtagCheckedForReplication;
    }

    public void setLastEtagCheckedForReplication(Etag lastEtagCheckedForReplication) {
      this.lastEtagCheckedForReplication = lastEtagCheckedForReplication;
    }

    public Etag getLastReplicatedEtag() {
      return lastReplicatedEtag;
    }

    public void setLastReplicatedEtag(Etag lastReplicatedEtag) {
      this.lastReplicatedEtag = lastReplicatedEtag;
    }

    public Date getLastReplicatedLastModified() {
      return lastReplicatedLastModified;
    }

    public void setLastReplicatedLastModified(Date lastReplicatedLastModified) {
      this.lastReplicatedLastModified = lastReplicatedLastModified;
    }

    public Date getLastSuccessTimestamp() {
      return lastSuccessTimestamp;
    }

    public void setLastSuccessTimestamp(Date lastSuccessTimestamp) {
      this.lastSuccessTimestamp = lastSuccessTimestamp;
    }

    public Date getLastFailureTimestamp() {
      return lastFailureTimestamp;
    }

    public void setLastFailureTimestamp(Date lastFailureTimestamp) {
      this.lastFailureTimestamp = lastFailureTimestamp;
    }

    public String getLastError() {
      return lastError;
    }

    public void setLastError(String lastError) {
      this.lastError = lastError;
    }

    private String lastError;

    public int getFailureCount() {
      return this.failureCountInternal;
    }
  }

}
