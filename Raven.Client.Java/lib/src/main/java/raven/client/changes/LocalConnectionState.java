package raven.client.changes;

import java.util.ArrayList;
import java.util.List;

import raven.abstractions.basic.EventHelper;
import raven.abstractions.basic.ExceptionEventArgs;
import raven.abstractions.closure.Action0;
import raven.abstractions.closure.Action1;
import raven.abstractions.data.BulkInsertChangeNotification;
import raven.abstractions.data.DocumentChangeNotification;
import raven.abstractions.data.IndexChangeNotification;
import raven.abstractions.data.ReplicationConflictNotification;


public class LocalConnectionState {
  private final Action0 onZero;
  private int value;

  private List<Action1<DocumentChangeNotification>> onDocumentChangeNotification = new ArrayList<>();
  private List<Action1<BulkInsertChangeNotification>> onBulkInsertChangeNotification = new ArrayList<>();
  private List<Action1<IndexChangeNotification>> onIndexChangeNotification = new ArrayList<>();
  private List<Action1<ReplicationConflictNotification>> onReplicationConflictNotification = new ArrayList<>();

  private List<Action1<ExceptionEventArgs>> onError = new ArrayList<>();


  public List<Action1<DocumentChangeNotification>> getOnDocumentChangeNotification() {
    return onDocumentChangeNotification;
  }

  public List<Action1<BulkInsertChangeNotification>> getOnBulkInsertChangeNotification() {
    return onBulkInsertChangeNotification;
  }

  public List<Action1<IndexChangeNotification>> getOnIndexChangeNotification() {
    return onIndexChangeNotification;
  }

  public List<Action1<ReplicationConflictNotification>> getOnReplicationConflictNotification() {
    return onReplicationConflictNotification;
  }

  public List<Action1<ExceptionEventArgs>> getOnError() {
    return onError;
  }

  public void send(DocumentChangeNotification documentChangeNotification) {
    EventHelper.invoke(onDocumentChangeNotification, documentChangeNotification);
  }

  public void send(IndexChangeNotification indexChangeNotification) {
    EventHelper.invoke(onIndexChangeNotification, indexChangeNotification);
  }

  public void send(ReplicationConflictNotification replicationConflictNotification) {
    EventHelper.invoke(onReplicationConflictNotification, replicationConflictNotification);
  }

  public void send(BulkInsertChangeNotification bulkInsertChangeNotification) {
    EventHelper.invoke(onBulkInsertChangeNotification, bulkInsertChangeNotification);
  }

  public void error(Exception e) {
    EventHelper.invoke(onError, new ExceptionEventArgs(e));
  }

  public LocalConnectionState(Action0 onZero) {
    value =0;
    this.onZero = onZero;
  }

  public void inc() {
    synchronized (this) {
      value++;
    }
  }

  public void dec() {
    synchronized (this) {
     if (--value == 0) {
       onZero.apply();
     }
    }
  }

}
