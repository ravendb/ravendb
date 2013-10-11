package raven.client.delegates;

import raven.client.connection.ReplicationInformer;


public interface ReplicationInformerFactory {
  public ReplicationInformer create(String url);
}
