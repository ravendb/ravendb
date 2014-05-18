package net.ravendb.client.delegates;

import net.ravendb.client.connection.IDocumentStoreReplicationInformer;

public interface ReplicationInformerFactory {
  public IDocumentStoreReplicationInformer create(String url);
}
