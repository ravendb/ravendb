package net.ravendb.client.delegates;

import net.ravendb.client.connection.ReplicationInformer;


public interface ReplicationInformerFactory {
  public ReplicationInformer create(String url);
}
