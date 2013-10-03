package raven.client.changes;

import raven.abstractions.closure.Action0;
import raven.client.connection.ReplicationInformer;
import raven.client.connection.implementation.HttpJsonRequestFactory;
import raven.client.document.DocumentConvention;


public class RemoteDatabaseChanges implements IDatabaseChanges {
//TODO:

  public RemoteDatabaseChanges(String url, HttpJsonRequestFactory jsonRequestFactory, DocumentConvention convertions,
    ReplicationInformer replicationInformer, Action0 onDispose) {
    //TODO: one more param !:  Func<string, Etag, string[], string, Task<bool>> tryResolveConflictByUsingRegisteredConflictListenersAsync
  }
}
