package net.ravendb.client.connection;

import net.ravendb.abstractions.replication.ReplicationDestination;


public interface IDocumentStoreReplicationInformer extends IReplicationInformerBase<ServerClient> {

  public ReplicationDestination[] getFailoverServers();

  public void setFailoverServers(ReplicationDestination[] failoverServers);

}
