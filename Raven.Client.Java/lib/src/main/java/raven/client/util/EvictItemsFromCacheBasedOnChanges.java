package raven.client.util;

import java.io.Closeable;

import raven.abstractions.closure.Action1;
import raven.abstractions.data.DocumentChangeNotification;
import raven.abstractions.data.DocumentChangeTypes;
import raven.abstractions.data.IndexChangeNotification;
import raven.abstractions.data.IndexChangeTypes;
import raven.client.changes.IDatabaseChanges;
import raven.client.changes.IObservable;
import raven.client.changes.IObserver;
import raven.client.changes.RemoteDatabaseChanges;

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
