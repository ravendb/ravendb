package net.ravendb.client.connection;

import java.io.Closeable;
import java.util.Date;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;

import net.ravendb.abstractions.basic.EventHandler;
import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.closure.Function1;
import net.ravendb.abstractions.connection.OperationCredentials;
import net.ravendb.abstractions.data.HttpMethods;
import net.ravendb.abstractions.exceptions.ServerClientException;
import net.ravendb.client.connection.ReplicationInformer.FailoverStatusChangedEventArgs;


public interface IReplicationInformerBase<T> extends Closeable {
  public void addFailoverStatusChanged(EventHandler<FailoverStatusChangedEventArgs> event);

  public void removeFailoverStatusChanged(EventHandler<FailoverStatusChangedEventArgs> event);

  public List<OperationMetadata> getReplicationDestinations();

  public List<OperationMetadata> getReplicationDestinationsUrls();

  /**
   * Updates the replication information if needed.
   */
  public void updateReplicationInformationIfNeeded(T client);

  /**
   * Refreshes the replication information.
   * @param client
   */
  public void refreshReplicationInformation(T client);

  /**
   * Get the current failure count for the url
   * @param operationUrl
   * @return
   */
  public AtomicLong getFailureCount(String operationUrl);

  /**
   * Get failure last check time for the url
   * @param operationUrl
   * @return
   */
  public Date getFailureLastCheck(String operationUrl);

  public int getReadStripingBase();

  public <S> S executeWithReplication(HttpMethods method, String primaryUrl, OperationCredentials primaryCredentials, int currentRequest,
    int currentReadStripingBase, Function1<OperationMetadata, S> operation) throws ServerClientException;

  public void forceCheck(String primaryUrl, boolean shouldForceCheck);

  public boolean isServerDown(Exception e, Reference<Boolean> timeout);

  public boolean isHttpStatus(Exception e, int... httpStatusCode);
}
