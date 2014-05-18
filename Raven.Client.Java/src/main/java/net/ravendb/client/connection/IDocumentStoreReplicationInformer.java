package net.ravendb.client.connection;

import net.ravendb.abstractions.replication.ReplicationDestination;


public interface IDocumentStoreReplicationInformer extends IReplicationInformerBase<ServerClient> {

  public ReplicationDestination[] getFailoverServers();

  public void setFailoverServers(ReplicationDestination[] failoverServers);

  /**
   * Updates replication information if needed
   */
  @Override
  public void updateReplicationInformationIfNeeded(ServerClient serverClient);

  /**
   * Updates replication information
   */
  @Override
  public void refreshReplicationInformation(ServerClient serverClient);

}
