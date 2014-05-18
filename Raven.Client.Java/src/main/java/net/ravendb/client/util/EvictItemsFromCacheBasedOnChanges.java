package net.ravendb.client.util;

import java.io.Closeable;

import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.data.DocumentChangeNotification;
import net.ravendb.abstractions.data.DocumentChangeTypes;
import net.ravendb.abstractions.data.IndexChangeNotification;
import net.ravendb.abstractions.data.IndexChangeTypes;
import net.ravendb.client.changes.IDatabaseChanges;
import net.ravendb.client.changes.IObservable;
import net.ravendb.client.changes.IObserver;
import net.ravendb.client.changes.RemoteDatabaseChanges;


public class EvictItemsFromCacheBasedOnChanges implements AutoCloseable {

  private final String databaseName;
  private final IDatabaseChanges changes;
  private final Action1<String> evictCacheOldItems;
  private final Closeable documentsSubscription;
  private final Closeable indexesSubscriptions;


  private abstract class ObserverAdapter<T> implements IObserver<T> {
    @Override
    public void onError(Exception error) {
      //empty by design
    }

    @Override
    public void onCompleted() {
      //empty by design
    }
  }
  private class DocumentChangeObserver extends ObserverAdapter<DocumentChangeNotification> {
    @Override
    public void onNext(DocumentChangeNotification value) {
      if (value.getType().equals(DocumentChangeTypes.PUT) || value.getType().equals(DocumentChangeTypes.DELETE)) {
        evictCacheOldItems.apply(databaseName);
      }
    }
  }
  private class IndexChangeObserver extends ObserverAdapter<IndexChangeNotification> {
    @Override
    public void onNext(IndexChangeNotification value) {
      if (value.getType().equals(IndexChangeTypes.MAP_COMPLETED)
        || value.getType().equals(IndexChangeTypes.REDUCE_COMPLETED)
        || value.getType().equals(IndexChangeTypes.INDEX_REMOVED)) {
        evictCacheOldItems.apply(databaseName);
      }
    }
  }


  public EvictItemsFromCacheBasedOnChanges(String databaseName, IDatabaseChanges changes, Action1<String> evictCacheOldItems) {
    this.databaseName = databaseName;
    this.changes = changes;
    this.evictCacheOldItems = evictCacheOldItems;
    IObservable<DocumentChangeNotification> docSub = changes.forAllDocuments();
    documentsSubscription = docSub.subscribe(new DocumentChangeObserver());
    IObservable<IndexChangeNotification> indexSub = changes.forAllIndexes();
    indexesSubscriptions = indexSub.subscribe(new IndexChangeObserver());
  }

  @Override
  public void close() throws Exception {
    documentsSubscription.close();
    indexesSubscriptions.close();

    try (AutoCloseable changes = (AutoCloseable) this.changes) {
      RemoteDatabaseChanges remoteDatabaseChanges = (RemoteDatabaseChanges) changes;
      if (remoteDatabaseChanges != null) {
        remoteDatabaseChanges.close();
      }
    }
  }

}
