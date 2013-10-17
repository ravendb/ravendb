package net.ravendb.client.changes;

import java.io.Closeable;
import java.io.IOException;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Timer;
import java.util.TimerTask;
import java.util.UUID;
import java.util.concurrent.ConcurrentSkipListSet;
import java.util.concurrent.TimeoutException;

import net.ravendb.abstractions.basic.EventArgs;
import net.ravendb.abstractions.basic.EventHandler;
import net.ravendb.abstractions.basic.EventHelper;
import net.ravendb.abstractions.basic.ExceptionEventArgs;
import net.ravendb.abstractions.basic.Reference;
import net.ravendb.abstractions.basic.VoidArgs;
import net.ravendb.abstractions.closure.Action0;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.closure.Function1;
import net.ravendb.abstractions.closure.Function4;
import net.ravendb.abstractions.closure.Predicate;
import net.ravendb.abstractions.closure.Predicates;
import net.ravendb.abstractions.data.BulkInsertChangeNotification;
import net.ravendb.abstractions.data.DocumentChangeNotification;
import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.HttpMethods;
import net.ravendb.abstractions.data.IndexChangeNotification;
import net.ravendb.abstractions.data.ReplicationConflictNotification;
import net.ravendb.abstractions.data.ReplicationConflictTypes;
import net.ravendb.abstractions.extensions.JsonExtensions;
import net.ravendb.abstractions.json.linq.RavenJObject;
import net.ravendb.abstractions.logging.ILog;
import net.ravendb.abstractions.logging.LogManager;
import net.ravendb.abstractions.util.AtomicDictionary;
import net.ravendb.abstractions.util.Base62Util;
import net.ravendb.client.connection.CreateHttpJsonRequestParams;
import net.ravendb.client.connection.RavenUrlExtensions;
import net.ravendb.client.connection.ReplicationInformer;
import net.ravendb.client.connection.implementation.HttpJsonRequest;
import net.ravendb.client.connection.implementation.HttpJsonRequestFactory;
import net.ravendb.client.document.DocumentConvention;
import net.ravendb.client.utils.UrlUtils;

import org.apache.commons.lang.StringUtils;
import org.apache.http.HttpStatus;
import org.codehaus.jackson.map.ObjectMapper;



public class RemoteDatabaseChanges implements IDatabaseChanges, AutoCloseable, IObserver<String> {

  private static final ILog logger = LogManager.getCurrentClassLogger();
  private final ConcurrentSkipListSet<String> watchedDocs = new ConcurrentSkipListSet<>();
  private final ConcurrentSkipListSet<String> watchedPrefixes = new ConcurrentSkipListSet<>();
  private final ConcurrentSkipListSet<String> watchedIndexes = new ConcurrentSkipListSet<>();
  private final ConcurrentSkipListSet<String> watchedBulkInserts = new ConcurrentSkipListSet<>();
  private boolean watchAllDocs;
  private boolean watchAllIndexes;

  private Timer clientSideHeartbeatTimer;

  private final String url;
  private final HttpJsonRequestFactory jsonRequestFactory;
  private final DocumentConvention conventions;
  private final ReplicationInformer replicationInformer;
  private final Action0 onDispose;
  private final Function4<String, Etag, String[] , String, Boolean> tryResolveConflictByUsingRegisteredConflictListeners;
  private final AtomicDictionary<LocalConnectionState> counters = new AtomicDictionary<>(String.CASE_INSENSITIVE_ORDER);
  private Closeable connection;
  private Date lastHeartbeat = new Date();

  private static int connectionCounter;
  private final String id;

  private boolean connected;

  private List<EventHandler<VoidArgs>> connectionStatusChanged;

  private volatile boolean disposed;


  @Override
  public void addConnectionStatusChanged(EventHandler<VoidArgs> handler) {
    connectionStatusChanged.add(handler);
  }

  @Override
  public void removeConnectionStatusChanges(EventHandler<VoidArgs> handler) {
    connectionStatusChanged.remove(handler);
  }

  @Override
  public boolean isConnected() {
    return connected;
  }



  public RemoteDatabaseChanges(String url, HttpJsonRequestFactory jsonRequestFactory, DocumentConvention conventions,
    ReplicationInformer replicationInformer, Action0 onDispose,
    Function4<String, Etag, String[], String, Boolean> tryResolveConflictByUsingRegisteredConflictListeners) {
    connectionStatusChanged = Arrays.<EventHandler<VoidArgs>> asList(new EventHandler<VoidArgs>() {
      @Override
      public void handle(Object sender, VoidArgs event) {
        logOnConnectionStatusChanged(sender, event);
      }
    });

    synchronized (RemoteDatabaseChanges.class) {
      connectionCounter++;

      id = connectionCounter + "/" + Base62Util.base62Random();
    }
    this.url = url;
    this.jsonRequestFactory = jsonRequestFactory;
    this.conventions = conventions;
    this.replicationInformer = replicationInformer;
    this.onDispose = onDispose;
    this.tryResolveConflictByUsingRegisteredConflictListeners = tryResolveConflictByUsingRegisteredConflictListeners;

    establishConnection();
  }

  @SuppressWarnings("null")
  public void establishConnection() {
    if (disposed) {
      return ;
    }

    if (clientSideHeartbeatTimer != null) {
      clientSideHeartbeatTimer.cancel();
      clientSideHeartbeatTimer = null;
    }

    CreateHttpJsonRequestParams requestParams = new CreateHttpJsonRequestParams(null, url + "/changes/events?id=" + id, HttpMethods.GET, null, conventions);
    requestParams.setAvoidCachingRequest(true);
    logger.info("Trying to connect to %s with id %s", requestParams.getUrl(), id);
    boolean retry = false;
    IObservable<String> serverEvents = null;
    try {
      serverEvents = jsonRequestFactory.createHttpJsonRequest(requestParams).serverPull();
    } catch (Exception e) {
      logger.warnException("Could not connect to server: " + url + " and id  " + id, e);
      connected = false;
      EventHelper.invoke(connectionStatusChanged, this, EventArgs.EMPTY);

      if (disposed) {
        throw e;
      }
      Reference<Boolean> timeoutRef = new Reference<>();
      if (!replicationInformer.isServerDown(e, timeoutRef)) {
        throw e;
      }
      if (replicationInformer.isHttpStatus(e, HttpStatus.SC_NOT_FOUND, HttpStatus.SC_FORBIDDEN, HttpStatus.SC_SERVICE_UNAVAILABLE)) {
        throw e;
      }
      logger.warn("Failed to connect to %s with id %s, will try again in 15 seconds", url, id);
      retry = true;
    }
    if (retry) {
      try {
        Thread.sleep(15000);
      } catch (InterruptedException e) {
        throw new IllegalStateException(e);
      }
      establishConnection();
      return;
    }

    if (disposed) {
      connected = false;
      EventHelper.invoke(connectionStatusChanged, this, EventArgs.EMPTY);
      throw new IllegalStateException("RemoteDatabaseChanges was disposed!");
    }

    connected = true;
    EventHelper.invoke(connectionStatusChanged, this, EventArgs.EMPTY);
    connection = (Closeable) serverEvents;
    serverEvents.subscribe(this);

    clientSideHeartbeatTimer = new Timer("Changes Client Heartbeat", true);
    clientSideHeartbeatTimer.schedule(new TimerTask() {
      @Override
      public void run() {
        clientSideHeartbeat();
      }
    }, 10000, 10000);

    if (watchAllDocs) {
      send("watch-docs", null);
    }
    if (watchAllIndexes) {
      send("watch-indexes", null);
    }
    for (String watchedDoc : watchedDocs) {
      send("watch-doc", watchedDoc);
    }
    for (String watchedPrefix : watchedPrefixes) {
      send("watch-prefix", watchedPrefix);
    }
    for (String watchedIndex : watchedIndexes) {
      send("watch-indexes", watchedIndex);
    }
    for (String watchedBulkInsert : watchedBulkInserts) {
      send("watch-bulk-operation", watchedBulkInsert);
    }
  }

  private void clientSideHeartbeat() {
    long elapsedTimeSinceHeartbeat = new Date().getTime() - lastHeartbeat.getTime();
    if (elapsedTimeSinceHeartbeat < 45 * 1000) {
      return;
    }
    onError(new TimeoutException("Over 45 seconds have passed since we got a server heartbeat, even though we should get one every 10 seconds or so.\r\n This connection is now presumed dead, and will attempt reconnection"));
  }

  private void logOnConnectionStatusChanged(Object sender, EventArgs eventArgs) {
    logger.info("Connection (%s) status changed, new status: %s", url, connected);
  }

  @Override
  public IObservable<IndexChangeNotification> forIndex(final String indexName) {
    LocalConnectionState counter = counters.getOrAdd("indexes/" + indexName, new Function1<String, LocalConnectionState>() {

      @Override
      public LocalConnectionState apply(String s) {
        watchedIndexes.add(indexName);
        send("watch-index", indexName);

        return new LocalConnectionState(new Action0() {
          @Override
          public void apply() {
            watchedIndexes.remove(indexName);
            send("unwatch-index", indexName);
            counters.remove("indexes/" + indexName);
          }
        });
      }
    });
    counter.inc();
    final TaskedObservable<IndexChangeNotification> taskedObservable = new TaskedObservable<>(counter, new Predicate<IndexChangeNotification>() {
      @Override
      public Boolean apply(IndexChangeNotification notification) {
        return notification.getName().equalsIgnoreCase(indexName);
      }
    });

    counter.getOnIndexChangeNotification().add(new Action1<IndexChangeNotification>() {
      @Override
      public void apply(IndexChangeNotification msg) {
        taskedObservable.send(msg);
      }
    });
    counter.getOnError().add(new Action1<ExceptionEventArgs>() {
      @Override
      public void apply(ExceptionEventArgs ex) {
        taskedObservable.error(ex.getException());
      }
    });
    return taskedObservable;
  }

  private void send(String command, String value) {
    synchronized (this) {
      logger.info("Sending command %s - %s to %s with id %s", command, value, url, id);

      try {
        String sendUrl = url + "/changes/config?id=" + id + "&command=" + command;
        if (StringUtils.isNotEmpty(value)) {
          sendUrl += "&value=" + UrlUtils.escapeUriString(value);
        }

        sendUrl = RavenUrlExtensions.noCache(sendUrl);

        CreateHttpJsonRequestParams requestParams = new CreateHttpJsonRequestParams(null, sendUrl, HttpMethods.GET, null, conventions);
        requestParams.setAvoidCachingRequest(true);
        HttpJsonRequest httpJsonRequest = jsonRequestFactory.createHttpJsonRequest(requestParams);
        httpJsonRequest.executeRequest();
      } catch (Exception e) {
        throw new RuntimeException(e);
      }
    }
  }

  @Override
  public IObservable<DocumentChangeNotification> forDocument(final String docId) {
    LocalConnectionState counter = counters.getOrAdd("docs/" + docId, new Function1<String, LocalConnectionState>() {
      @Override
      public LocalConnectionState apply(String s) {
        watchedDocs.add(docId);
        send("watch-doc", docId);

        return new LocalConnectionState(new Action0() {
          @Override
          public void apply() {
            watchedDocs.remove(docId);
            send("unwatch-doc", docId);
            counters.remove("docs/" + docId);
          }
        });
      }
    });

    final TaskedObservable<DocumentChangeNotification> taskedObservable = new TaskedObservable<>(counter, new Predicate<DocumentChangeNotification>() {
      @Override
      public Boolean apply(DocumentChangeNotification notification) {
        return notification.getId().equalsIgnoreCase(docId);
      }
    });

    counter.getOnDocumentChangeNotification().add(new Action1<DocumentChangeNotification>() {
      @Override
      public void apply(DocumentChangeNotification msg) {
        taskedObservable.send(msg);
      }
    });
    counter.getOnError().add(new Action1<ExceptionEventArgs>() {
      @Override
      public void apply(ExceptionEventArgs ex) {
        taskedObservable.error(ex.getException());
      }
    });
    return taskedObservable;
  }

  @Override
  public IObservable<DocumentChangeNotification> forAllDocuments() {
    LocalConnectionState counter = counters.getOrAdd("all-docs", new Function1<String, LocalConnectionState>() {
      @Override
      public LocalConnectionState apply(String s) {
        watchAllDocs = true;
        send("watch-docs", null);

        return new LocalConnectionState(new Action0() {
          @Override
          public void apply() {
            watchAllDocs = false;
            send("unwatch-docs", null);
            counters.remove("all-docs");
          }
        });
      }
    });

    final TaskedObservable<DocumentChangeNotification> taskedObservable = new TaskedObservable<>(counter, Predicates.<DocumentChangeNotification> alwaysTrue());

    counter.getOnDocumentChangeNotification().add(new Action1<DocumentChangeNotification>() {
      @Override
      public void apply(DocumentChangeNotification msg) {
        taskedObservable.send(msg);
      }
    });
    counter.getOnError().add(new Action1<ExceptionEventArgs>() {
      @Override
      public void apply(ExceptionEventArgs ex) {
        taskedObservable.error(ex.getException());
      }
    });
    return taskedObservable;
  }

  @Override
  public IObservable<BulkInsertChangeNotification> forBulkInsert(final UUID operationId) {

    final String id = operationId.toString();

    LocalConnectionState counter = counters.getOrAdd("bulk-operations/" + id, new Function1<String, LocalConnectionState>() {
      @Override
      public LocalConnectionState apply(String s) {
        watchedBulkInserts.add(id);
        send("watch-bulk-operation", id);

        return new LocalConnectionState(new Action0() {
          @Override
          public void apply() {
            watchedBulkInserts.remove(id);
            send("unwatch-bulk-operation", id);
            counters.remove("bulk-operations/" + operationId);
          }
        });
      }
    });

    final TaskedObservable<BulkInsertChangeNotification> taskedObservable = new TaskedObservable<>(counter, new Predicate<BulkInsertChangeNotification>() {
      @Override
      public Boolean apply(BulkInsertChangeNotification notification) {
        return notification.getOperationId().equals(operationId);
      }
    });

    counter.getOnBulkInsertChangeNotification().add(new Action1<BulkInsertChangeNotification>() {
      @Override
      public void apply(BulkInsertChangeNotification msg) {
        taskedObservable.send(msg);
      }
    });
    counter.getOnError().add(new Action1<ExceptionEventArgs>() {
      @Override
      public void apply(ExceptionEventArgs ex) {
        taskedObservable.error(ex.getException());
      }
    });
    return taskedObservable;
  }

  @Override
  public IObservable<IndexChangeNotification> forAllIndexes() {
    LocalConnectionState counter = counters.getOrAdd("all-indexes", new Function1<String, LocalConnectionState>() {

      @Override
      public LocalConnectionState apply(String s) {
        watchAllIndexes = true;
        send("watch-indexes", null);

        return new LocalConnectionState(new Action0() {
          @Override
          public void apply() {
            watchAllIndexes = false;
            send("unwatch-indexes", null);
            counters.remove("all-indexes");
          }
        });
      }
    });
    counter.inc();
    final TaskedObservable<IndexChangeNotification> taskedObservable = new TaskedObservable<>(counter, Predicates.<IndexChangeNotification> alwaysTrue());

    counter.getOnIndexChangeNotification().add(new Action1<IndexChangeNotification>() {
      @Override
      public void apply(IndexChangeNotification msg) {
        taskedObservable.send(msg);
      }
    });
    counter.getOnError().add(new Action1<ExceptionEventArgs>() {
      @Override
      public void apply(ExceptionEventArgs ex) {
        taskedObservable.error(ex.getException());
      }
    });
    return taskedObservable;
  }

  @Override
  public IObservable<DocumentChangeNotification> forDocumentsStartingWith(final String docIdPrefix) {
    LocalConnectionState counter = counters.getOrAdd("prefixes" + docIdPrefix, new Function1<String, LocalConnectionState>() {
      @Override
      public LocalConnectionState apply(String s) {
        watchedPrefixes.add(docIdPrefix);
        send("watch-prefix", docIdPrefix);

        return new LocalConnectionState(new Action0() {
          @Override
          public void apply() {
            watchedPrefixes.remove(docIdPrefix);
            send("unwatch-prefix", docIdPrefix);
            counters.remove("prefixes/" + docIdPrefix);
          }
        });
      }
    });

    final TaskedObservable<DocumentChangeNotification> taskedObservable = new TaskedObservable<>(counter, new Predicate<DocumentChangeNotification>() {
      @Override
      public Boolean apply(DocumentChangeNotification notification) {
        return notification.getId() != null && notification.getId().toLowerCase().startsWith(docIdPrefix.toLowerCase());
      }
    });

    counter.getOnDocumentChangeNotification().add(new Action1<DocumentChangeNotification>() {
      @Override
      public void apply(DocumentChangeNotification msg) {
        taskedObservable.send(msg);
      }
    });
    counter.getOnError().add(new Action1<ExceptionEventArgs>() {
      @Override
      public void apply(ExceptionEventArgs ex) {
        taskedObservable.error(ex.getException());
      }
    });
    return taskedObservable;
  }

  @Override
  public IObservable<ReplicationConflictNotification> forAllReplicationConflicts() {
    LocalConnectionState counter = counters.getOrAdd("all-replication-conflicts", new Function1<String, LocalConnectionState>() {
      @Override
      public LocalConnectionState apply(String s) {
        watchAllIndexes = true;
        send("watch-replication-conflicts", null);

        return new LocalConnectionState(new Action0() {
          @Override
          public void apply() {
            watchAllIndexes = false;
            send("unwatch-replication-conflicts", null);
            counters.remove("all-replication-conflicts");
          }
        });
      }
    });

    final TaskedObservable<ReplicationConflictNotification> taskedObservable = new TaskedObservable<>(counter, Predicates.<ReplicationConflictNotification> alwaysTrue());

    counter.getOnReplicationConflictNotification().add(new Action1<ReplicationConflictNotification>() {
      @Override
      public void apply(ReplicationConflictNotification msg) {
        taskedObservable.send(msg);
      }
    });
    counter.getOnError().add(new Action1<ExceptionEventArgs>() {
      @Override
      public void apply(ExceptionEventArgs ex) {
        taskedObservable.error(ex.getException());
      }
    });
    return taskedObservable;
  }

  @Override
  public void waitForAllPendingSubscriptions() {
    // this method simply returns as we process requests synchronically
  }

  @Override
  public void close() {
    if (disposed) {
      return;
    }
    disposed = true;
    onDispose.apply();

    if (clientSideHeartbeatTimer != null) {
      clientSideHeartbeatTimer.cancel();
    }
    clientSideHeartbeatTimer = null;

    send("disconnect", null);

    try {
      if (connection != null) {
        connection.close();
      }
    } catch (Exception e) {
      logger.errorException("Got error from server connection for " + url + " on id " + id , e);
    }
  }

  @Override
  public void onNext(String dataFromConnection) {
    lastHeartbeat = new Date();
    RavenJObject ravenJObject = RavenJObject.parse(dataFromConnection);
    RavenJObject value = ravenJObject.value(RavenJObject.class, "Value");
    String type = ravenJObject.value(String.class, "Type");

    logger.debug("Got notification from %s id %s of type %s", url, id, dataFromConnection);

    ObjectMapper mapper = JsonExtensions.createDefaultJsonSerializer();

    try {
      switch (type) {
        case "DocumentChangeNotification":
          DocumentChangeNotification documentChangeNotification = mapper.readValue(value.toString(), DocumentChangeNotification.class);
          for (LocalConnectionState counter : counters.values()) {
            counter.send(documentChangeNotification);
          }
          break;

        case "BulkInsertChangeNotification":
          BulkInsertChangeNotification bulkInsertChangeNotification = mapper.readValue(value.toString(), BulkInsertChangeNotification.class);
          for (LocalConnectionState counter : counters.values()) {
            counter.send(bulkInsertChangeNotification);
          }
          break;

        case "IndexChangeNotification":
          IndexChangeNotification indexChangeNotification = mapper.readValue(value.toString(), IndexChangeNotification.class);
          for (LocalConnectionState counter : counters.values()) {
            counter.send(indexChangeNotification);
          }
          break;
        case "ReplicationConflictNotification":
          ReplicationConflictNotification replicationConflictNotification = mapper.readValue(value.toString(), ReplicationConflictNotification.class);
          for (LocalConnectionState counter: counters.values()) {
            counter.send(replicationConflictNotification);
          }
          if (replicationConflictNotification.getItemType().equals(ReplicationConflictTypes.DOCUMENT_REPLICATION_CONFLICT)) {
            boolean result = tryResolveConflictByUsingRegisteredConflictListeners.apply(replicationConflictNotification.getId(),
                replicationConflictNotification.getEtag(), replicationConflictNotification.getConflicts(), null);
            if (result) {
              logger.debug("Document replication conflict for %s was resolved by one of the registered conflict listeners",
                replicationConflictNotification.getId());
            }
          }
          break;
        case "Disconnect":
          if (connection != null) {
            connection.close();
          }
          renewConnection();
          break;
        case "Initialized":
        case "Heartbeat":
          break;
        default:
          break;
      }
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void onError(Exception error) {
    logger.errorException("Got error from server connection for " + url + " on id " + id, error);
    renewConnection();
  }

  private void renewConnection() {
    try {
      Thread.sleep(15000);
    } catch (InterruptedException e) {
      // ignore
    }
    try {
      establishConnection();
    } catch (Exception e) {
      for (Map.Entry<String, LocalConnectionState> keyValuePair : counters) {
        keyValuePair.getValue().error(e);
      }
      counters.clear();
    }
  }

  @Override
  public void onCompleted() {
    //empty by design
  }



}
