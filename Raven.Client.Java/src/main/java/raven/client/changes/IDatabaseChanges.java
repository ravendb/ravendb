package raven.client.changes;

import java.util.UUID;

import raven.abstractions.basic.EventHandler;
import raven.abstractions.basic.VoidArgs;
import raven.abstractions.data.BulkInsertChangeNotification;
import raven.abstractions.data.DocumentChangeNotification;
import raven.abstractions.data.IndexChangeNotification;
import raven.abstractions.data.ReplicationConflictNotification;

public interface IDatabaseChanges {

  public boolean isConnected();

  public void addConnectionStatusChanged(EventHandler<VoidArgs> handler);

  public void removeConnectionStatusChanges(EventHandler<VoidArgs> handler);

  public IObservable<IndexChangeNotification> forIndex(String indexName);

  public IObservable<DocumentChangeNotification> forDocument(String docId);

  public IObservable<DocumentChangeNotification> forAllDocuments();

  public IObservable<IndexChangeNotification> forAllIndexes();

  public IObservable<DocumentChangeNotification> forDocumentsStartingWith(String docIdPrefix);

  public IObservable<ReplicationConflictNotification> forAllReplicationConflicts();

  public IObservable<BulkInsertChangeNotification> forBulkInsert(UUID operationId);

  void waitForAllPendingSubscriptions();
}
