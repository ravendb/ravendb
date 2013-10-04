package raven.client.changes;

import java.util.UUID;

import raven.abstractions.data.BulkInsertChangeNotification;
import raven.abstractions.data.DocumentChangeNotification;
import raven.abstractions.data.IndexChangeNotification;
import raven.abstractions.data.ReplicationConflictNotification;

public interface IDatabaseChanges {

  public boolean isConnected();

  //TODO: event EventHandler ConnectionStatusChanged;

  public IObservable<IndexChangeNotification> forIndex(String indexName);

  public IObservable<DocumentChangeNotification> forDocument(String docId);

  public IObservable<DocumentChangeNotification> forAllDocuments();

  public IObservable<IndexChangeNotification> forAllIndexes();

  public IObservable<DocumentChangeNotification> forDocumentsStartingWith(String docIdPrefix);

  public IObservable<ReplicationConflictNotification> forAllReplicationConflicts();

  public IObservable<BulkInsertChangeNotification> forBulkInsert(UUID operationId);

  void waitForAllPendingSubscriptions();
}
