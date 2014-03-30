package net.ravendb.client.document;

import java.io.Closeable;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ConcurrentSkipListMap;

import net.ravendb.abstractions.basic.EventHandler;
import net.ravendb.abstractions.basic.EventHelper;
import net.ravendb.abstractions.basic.VoidArgs;
import net.ravendb.abstractions.closure.Action0;
import net.ravendb.abstractions.closure.Action1;
import net.ravendb.abstractions.closure.Function0;
import net.ravendb.abstractions.closure.Function1;
import net.ravendb.abstractions.closure.Function4;
import net.ravendb.abstractions.connection.OperationCredentials;
import net.ravendb.abstractions.connection.WebRequestEventArgs;
import net.ravendb.abstractions.data.BulkInsertOptions;
import net.ravendb.abstractions.data.ConnectionStringParser;
import net.ravendb.abstractions.data.Constants;
import net.ravendb.abstractions.data.Etag;
import net.ravendb.abstractions.data.RavenConnectionStringOptions;
import net.ravendb.abstractions.oauth.BasicAuthenticator;
import net.ravendb.abstractions.oauth.SecuredAuthenticator;
import net.ravendb.abstractions.util.AtomicDictionary;
import net.ravendb.client.DocumentStoreBase;
import net.ravendb.client.IDocumentSession;
import net.ravendb.client.IDocumentStore;
import net.ravendb.client.changes.IDatabaseChanges;
import net.ravendb.client.changes.RemoteDatabaseChanges;
import net.ravendb.client.connection.IDatabaseCommands;
import net.ravendb.client.connection.IDocumentStoreReplicationInformer;
import net.ravendb.client.connection.IReplicationInformerBase;
import net.ravendb.client.connection.OperationMetadata;
import net.ravendb.client.connection.ReplicationInformer;
import net.ravendb.client.connection.ServerClient;
import net.ravendb.client.connection.implementation.HttpJsonRequestFactory;
import net.ravendb.client.connection.profiling.RequestResultArgs;
import net.ravendb.client.delegates.HttpResponseWithMetaHandler;
import net.ravendb.client.extensions.MultiDatabase;
import net.ravendb.client.listeners.IDocumentConflictListener;
import net.ravendb.client.util.EvictItemsFromCacheBasedOnChanges;
import net.ravendb.client.utils.Closer;
import net.ravendb.client.utils.RequirementsChecker;

import org.apache.commons.lang.StringUtils;
import org.apache.http.Header;
import org.apache.http.HttpRequest;
import org.apache.http.HttpResponse;


/**
 * Manages access to RavenDB and open sessions to work with RavenDB.
 */
public class DocumentStore extends DocumentStoreBase {

  // The current session id - only used during construction
  protected static ThreadLocal<UUID> currentSessionId = new ThreadLocal<>();

  private final static int DEFAULT_NUMBER_OF_CACHED_REQUESTS = 2048;
  private int maxNumberOfCachedRequests = DEFAULT_NUMBER_OF_CACHED_REQUESTS;
  private boolean aggressiveCachingUsed;

  protected Function0<IDatabaseCommands> databaseCommandsGenerator;

  private final ConcurrentMap<String, IDocumentStoreReplicationInformer> replicationInformers =  new ConcurrentSkipListMap<>(String.CASE_INSENSITIVE_ORDER);
  private String identifier;

  private final AtomicDictionary<IDatabaseChanges> databaseChanges = new AtomicDictionary<>(String.CASE_INSENSITIVE_ORDER);
  protected HttpJsonRequestFactory jsonRequestFactory = new HttpJsonRequestFactory(DEFAULT_NUMBER_OF_CACHED_REQUESTS);


  private ConcurrentMap<String, EvictItemsFromCacheBasedOnChanges> observeChangesAndEvictItemsFromCacheForDatabases = new ConcurrentHashMap<>();

  private String apiKey;
  private String defaultDatabase;

  /**
   * Called after dispose is completed
   */
  private List<EventHandler<VoidArgs>> afterDispose = new ArrayList<>();


  /**
   * Called after dispose is completed
   * @param event
   */
  @Override
  public void addAfterDisposeEventHandler(EventHandler<VoidArgs> event) {
    this.afterDispose.add(event);
  }

  public String getDefaultDatabase() {
    return defaultDatabase;
  }

  public void setDefaultDatabase(String defaultDatabase) {
    this.defaultDatabase = defaultDatabase;
  }

  /**
   * Remove event handler
   * @param event
   */
  @Override
  public void removeAfterDisposeEventHandler(EventHandler<VoidArgs> event) {
    this.afterDispose.remove(event);
  }


  @Override
  public boolean hasJsonRequestFactory() {
    return true;
  }

  @Override
  public HttpJsonRequestFactory getJsonRequestFactory() {
    return jsonRequestFactory;
  }

  @Override
  public IDatabaseCommands getDatabaseCommands() {
    assertInitialized();
    IDatabaseCommands commands = databaseCommandsGenerator.apply();
    for (String key: getSharedOperationsHeaders().keySet()) {
      String value = getSharedOperationsHeaders().get(key);
      if (value == null) {
        continue;
      }
      commands.getOperationsHeaders().put(key, value);
    }
    return commands;
  }

  public DocumentStore() {
    setSharedOperationsHeaders(new HashMap<String, String>());
    setConventions(new DocumentConvention());
  }

  public DocumentStore(String url) {
    this();
    this.url = url;
  }

  public DocumentStore(String url, String defaultDb) {
    this();
    this.url = url;
    this.defaultDatabase = defaultDb;
  }

  @Override
  public String getIdentifier() {
    if (identifier != null) {
      return identifier;
    }
    if (getUrl() == null) {
      return null;
    }
    if (defaultDatabase != null) {
      return getUrl() + " (DB: " + defaultDatabase + ")";
    }
    return url;
  }

  @Override
  public void setIdentifier(String value) {
    this.identifier = value;
  }

  public String getApiKey() {
    return apiKey;
  }

  public void setApiKey(String apiKey) {
    this.apiKey = apiKey;
  }

  /**
   *  Set document store settings based on a given connection string.
   *  Ex. Url=http://localhost:8123;
   * @param connString
   */
  public void parseConnectionString(String connString) {
    ConnectionStringParser<RavenConnectionStringOptions> connectionStringOptions = ConnectionStringParser.fromConnectionString(RavenConnectionStringOptions.class, connString);
    connectionStringOptions.parse();
    setConnectionStringSettings(connectionStringOptions.getConnectionStringOptions());
  }
  /**
   *  Copy the relevant connection string settings
   */
  protected void setConnectionStringSettings(RavenConnectionStringOptions options) {
    if (StringUtils.isNotEmpty(options.getUrl())) {
      setUrl(options.getUrl());
    }
    if (StringUtils.isNotEmpty(options.getDefaultDatabase())) {
      defaultDatabase = options.getDefaultDatabase();
    }
    if (StringUtils.isNotEmpty(options.getApiKey())) {
      apiKey = options.getApiKey();
    }
    if (options.getFailoverServers() != null) {
      failoverServers = options.getFailoverServers();
    }
  }


  @Override
  public void close() throws Exception {
    for (EvictItemsFromCacheBasedOnChanges observeChangesAndEvictItemsFromCacheForDatabase : observeChangesAndEvictItemsFromCacheForDatabases.values()) {
      observeChangesAndEvictItemsFromCacheForDatabase.close();
    }

    for (Map.Entry<String, IDatabaseChanges> databaseChange : databaseChanges) {
      IDatabaseChanges dbChange = databaseChange.getValue();
      if (dbChange instanceof RemoteDatabaseChanges) {
        ((RemoteDatabaseChanges) dbChange).close();
      } else {
        if (databaseChange.getValue() instanceof Closeable) {
          ((Closeable) databaseChange.getValue()).close();
        }
      }
    }

    for (IDocumentStoreReplicationInformer ri : replicationInformers.values()) {
      ri.close();
    }

    // if this is still going, we continue with disposal, it is for grace only, anyway

    if (jsonRequestFactory != null) {
      jsonRequestFactory.close();
    }

    setWasDisposed(true);
    if (afterDispose != null) {
      EventHelper.invoke(afterDispose, this, null);
    }
  }

  /**
   * Opens the session.
   */
  @Override
  public IDocumentSession openSession() {
    return openSession(new OpenSessionOptions());
  }

  /**
   * Opens the session for a particular database
   */
  @Override
  public IDocumentSession openSession(String database) {
    OpenSessionOptions opts = new OpenSessionOptions();
    opts.setDatabase(database);
    return openSession(opts);
  }

  @Override
  public IDocumentSession openSession(OpenSessionOptions options) {
    ensureNotClosed();

    UUID sessionId = UUID.randomUUID();
    currentSessionId.set(sessionId);
    try {
      DocumentSession session = new DocumentSession(options.getDatabase(), this, listeners, sessionId,
        setupCommands(getDatabaseCommands(), options.getDatabase(), options));
      session.setDatabaseName(options.getDatabase() != null ? options.getDatabase() : defaultDatabase);

      afterSessionCreated(session);
      return session;
    } finally {
      currentSessionId.set(null);
    }
  }

  private static IDatabaseCommands setupCommands(IDatabaseCommands databaseCommands, String database, OpenSessionOptions options) {
    if (database != null) {
      databaseCommands = databaseCommands.forDatabase(database);
    }
    if (options.isForceReadFromMaster()) {
      databaseCommands.forceReadFromMaster();
    }
    return databaseCommands;
  }

  @Override
  public IDocumentStore initialize() {
    if (initialized) {
      return this;
    }

    assertValidConfiguration();

    jsonRequestFactory = new HttpJsonRequestFactory(getMaxNumberOfCachedRequests());
    try {
      initializeEncryptor();
      initializeSecurity();

      initializeInternal();

      if (conventions.getDocumentKeyGenerator() == null) { // don't overwrite what the user is doing
        final MultiDatabaseHiLoGenerator generator = new MultiDatabaseHiLoGenerator(32);
        conventions.setDocumentKeyGenerator(new DocumentKeyGenerator() {
          @Override
          public String generate(String dbName, IDatabaseCommands databaseCommands, Object entity) {
            return generator.generateDocumentKey(dbName, databaseCommands, conventions, entity);
          }
        });
      }

      initialized = true;

      if (StringUtils.isNotEmpty(defaultDatabase) && !defaultDatabase.equals(Constants.SYSTEM_DATABASE)) { //system database exists anyway
        getDatabaseCommands().forSystemDatabase().getGlobalAdmin().ensureDatabaseExists(defaultDatabase, true);
      }
    } catch (Exception e) {
      Closer.close(this);
      throw e;
    }

    return this;
  }

  public void initializeProfiling() {
    if (jsonRequestFactory == null) {
      throw new IllegalStateException("Cannot call InitializeProfiling() before Initialize() was called.");
    }
    conventions.setDisableProfiling(false);
    jsonRequestFactory.addLogRequestEventHandler(new EventHandler<RequestResultArgs>() {

      @Override
      public void handle(Object sender, final RequestResultArgs args) {
        if (conventions.isDisableProfiling()) {
          return;
        }
        if (args.getTotalSize() > 1024 * 1024 * 2) {
          RequestResultArgs newArgs = new RequestResultArgs();
          newArgs.setUrl(args.getUrl());
          newArgs.setPostedData("total request/response size > 2MB, not tracked");
          newArgs.setResult("total request/response size > 2MB, not tracked");
          profilingContext.recordAction(sender, newArgs);
          return;
        }
        profilingContext.recordAction(sender, args);
      }
    });
  }

  private void initializeSecurity() {
    if (conventions.getHandleUnauthorizedResponse() != null) {
      return ; // already setup by the user
    }

    final BasicAuthenticator basicAuthenticator = new BasicAuthenticator(jsonRequestFactory.getHttpClient(), jsonRequestFactory.isEnableBasicAuthenticationOverUnsecuredHttpEvenThoughPasswordsWouldBeSentOverTheWireInClearTextToBeStolenByHackers());
    final SecuredAuthenticator securedAuthenticator = new SecuredAuthenticator(apiKey, jsonRequestFactory);

    jsonRequestFactory.addConfigureRequestEventHandler(new EventHandler<WebRequestEventArgs>() {
      @Override
      public void handle(Object sender, WebRequestEventArgs event) {
        basicAuthenticator.configureRequest(sender, event);
      }
    });
    jsonRequestFactory.addConfigureRequestEventHandler(new EventHandler<WebRequestEventArgs>() {
      @Override
      public void handle(Object sender, WebRequestEventArgs event) {
        securedAuthenticator.configureRequest(sender, event);
      }
    });

    conventions.setHandleUnauthorizedResponse(new HttpResponseWithMetaHandler() {
      @SuppressWarnings("null")
      @Override
      public Action1<HttpRequest> handle(HttpResponse response, OperationCredentials credentials) {
        Header oauthSourceHeader = response.getFirstHeader("OAuth-Source");
        String oauthSource = null;
        if (oauthSourceHeader != null) {
          oauthSource = oauthSourceHeader.getValue();
        }
        if (StringUtils.isNotEmpty(oauthSource) && !oauthSource.toLowerCase().endsWith("/OAuth/API-Key".toLowerCase())) {
          return basicAuthenticator.doOAuthRequest(oauthSource, credentials.getApiKey());
        }
        if (apiKey == null) {
          //AssertUnauthorizedCredentialSupportWindowsAuth(response);
          return null;
        }
        if (StringUtils.isEmpty(oauthSource)) {
          oauthSource = getUrl() + "/OAuth/API-Key";
        }
        return securedAuthenticator.doOAuthRequest(oauthSource, credentials.getApiKey());
      }
    });
  }

  /**
   * validate the configuration for the document store
   */
  protected void assertValidConfiguration() {
    if (StringUtils.isEmpty(url)) {
      throw new IllegalArgumentException("Document store URL cannot be empty");
    }
  }

  /**
   * Initialize the document store access method to RavenDB
   */
  protected void initializeInternal() {

    final String rootDatabaseUrl = MultiDatabase.getRootDatabaseUrl(url);
    databaseCommandsGenerator = new Function0<IDatabaseCommands>() {

      @Override
      public IDatabaseCommands apply() {
        String databaseUrl = getUrl();
        if (StringUtils.isNotEmpty(defaultDatabase)) {
          databaseUrl = rootDatabaseUrl;
          databaseUrl += "/databases/" + defaultDatabase;
        }

        return new ServerClient(databaseUrl, conventions, new OperationCredentials(apiKey),
          new ReplicationInformerGetter()
        , null, jsonRequestFactory, currentSessionId.get(), listeners.getConflictListeners().toArray(new IDocumentConflictListener[0]));
      }
    };

  }
  protected class ReplicationInformerGetter implements Function1<String, IDocumentStoreReplicationInformer> {

    @Override
    public IDocumentStoreReplicationInformer apply(String dbName) {
      return getReplicationInformerForDatabase(dbName);
    }

  }


  public IDocumentStoreReplicationInformer getReplicationInformerForDatabase() {
    return getReplicationInformerForDatabase(null);
  }

  public IDocumentStoreReplicationInformer getReplicationInformerForDatabase(String dbName) {
    String key = url;
    if (dbName == null) {
      dbName = defaultDatabase;
    }
    if (StringUtils.isNotEmpty(dbName)) {
      key = MultiDatabase.getRootDatabaseUrl(url) + "/databases/" + dbName;
    }
    replicationInformers.putIfAbsent(key, conventions.getReplicationInformerFactory().create(key));
    IDocumentStoreReplicationInformer informer = replicationInformers.get(key);

    if (failoverServers == null) {
      return informer;
    }

    if (dbName.equals(getDefaultDatabase())) {
      if (failoverServers.isSetForDefaultDatabase() && informer.getFailoverServers() == null) {
        informer.setFailoverServers(failoverServers.getForDefaultDatabase());
      }
    } else {
      if (failoverServers.isSetForDatabase(dbName) && informer.getFailoverServers() == null) {
        informer.setFailoverServers(failoverServers.getForDatabase(dbName));
      }
    }

    return informer;
  }

  /**
   * Setup the context for no aggressive caching
   *
   * This is mainly useful for internal use inside RavenDB, when we are executing
   * queries that have been marked with WaitForNonStaleResults, we temporarily disable
   * aggressive caching.
   */
  @Override
  public AutoCloseable disableAggressiveCaching() {
    assertInitialized();
    final Long old = jsonRequestFactory.getAggressiveCacheDuration();
    jsonRequestFactory.setAggressiveCacheDuration(null);
    return new AutoCloseable() {

      @Override
      public void close() throws Exception {
        jsonRequestFactory.setAggressiveCacheDuration(old);
      }
    };
  }

  /**
   *  Subscribe to change notifications from the server
   */
  @Override
  public IDatabaseChanges changes() {
    return changes(null);
  }

  /**
   * Subscribe to change notifications from the server
   * @param database
   * @return
   */
  @Override
  public IDatabaseChanges changes(String database) {
    assertInitialized();
    if (database == null) {
      database = defaultDatabase;
    }
    return databaseChanges.getOrAdd(database, new Function1<String, IDatabaseChanges>() {

      @Override
      public IDatabaseChanges apply(String database) {
        return createDatabaseChanges(database);
      }
    });
  }

  protected IDatabaseChanges createDatabaseChanges(String database) {
    if (StringUtils.isEmpty(url)) {
      throw new IllegalStateException("Changes API requires usage of server/client");
    }

    if (database == null) {
      database = defaultDatabase;
    }


    String dbUrl = MultiDatabase.getRootDatabaseUrl(url);
    if (StringUtils.isNotEmpty(database)) {
      dbUrl = dbUrl + "/databases/" + database;
    }
    final String databaseClousure = database;

    return new RemoteDatabaseChanges(dbUrl, apiKey,
      jsonRequestFactory,
      getConventions(),
      getReplicationInformerForDatabase(database),
      new Action0() {
      @Override
      public void apply() {
        databaseChanges.remove(databaseClousure);
      }
    }, new Function4<String, Etag, String[], OperationMetadata, Boolean>() {
      @Override
      public Boolean apply(String key, Etag etag, String[] conflictedIds, OperationMetadata opUrl) {
        return getDatabaseCommands().tryResolveConflictByUsingRegisteredListeners(key, etag, conflictedIds, opUrl);
      }
    }
      );
  }

  /**
   * Setup the context for aggressive caching.
   *
   * Aggressive caching means that we will not check the server to see whatever the response
   * we provide is current or not, but will serve the information directly from the local cache
   * without touching the server.
   */
  @Override
  public AutoCloseable aggressivelyCacheFor(long cacheDurationInMilis)
  {
    assertInitialized();
    if (cacheDurationInMilis < 1000)
      throw new IllegalArgumentException("cacheDuration must be longer than a single second");

    final Long old = jsonRequestFactory.getAggressiveCacheDuration();
    jsonRequestFactory.setAggressiveCacheDuration(cacheDurationInMilis);

    aggressiveCachingUsed = true;

    return new AutoCloseable() {

      @Override
      public void close() throws Exception {
        jsonRequestFactory.setAggressiveCacheDuration(old);
      }
    };
  }



  public int getMaxNumberOfCachedRequests() {
    return maxNumberOfCachedRequests;
  }

  public void setMaxNumberOfCachedRequests(int value) {
    maxNumberOfCachedRequests = value;
    if (jsonRequestFactory != null) {
      Closer.close(jsonRequestFactory);
    }
    jsonRequestFactory = new HttpJsonRequestFactory(maxNumberOfCachedRequests);
  }


  @Override
  public BulkInsertOperation bulkInsert() {
    return bulkInsert(null, null);
  }

  @Override
  public BulkInsertOperation bulkInsert(String database) {
    return bulkInsert(database, null);
  }

  @Override
  public BulkInsertOperation bulkInsert(String database, BulkInsertOptions options) {
    return new BulkInsertOperation(database != null ? database : getDefaultDatabase(), this, listeners, options != null ? options : new BulkInsertOptions(),
      changes(database != null ? database : getDefaultDatabase()));
  }

  @Override
  protected void afterSessionCreated(InMemoryDocumentSessionOperations session) {
    if (conventions.isShouldAggressiveCacheTrackChanges() && aggressiveCachingUsed) {
      String databaseName = session.getDatabaseName();
      if (databaseName == null) {
        databaseName = Constants.SYSTEM_DATABASE;
      }
      observeChangesAndEvictItemsFromCacheForDatabases.putIfAbsent(databaseName,
        new EvictItemsFromCacheBasedOnChanges(databaseName, changes(databaseName), new ExpireItemsFromCacheAction()));
    }

    super.afterSessionCreated(session);
  }
  private class ExpireItemsFromCacheAction implements Action1<String>  {

    @Override
    public void apply(String db) {
      jsonRequestFactory.expireItemsFromCache(db);
    }
  }

  public IDocumentStore useFips(boolean value) {
    this.useFips = value;
    return this;
  }

  public IDocumentStore withApiKey(String apiKey) {
    RequirementsChecker.checkOAuthDeps();
    this.apiKey = apiKey;
    return this;
  }

  /**
   * Setup the WebRequest timeout for the session
   */
  @Override
  public AutoCloseable setRequestsTimeoutFor(long timeout) {
    assertInitialized();

    final Long old = jsonRequestFactory.getRequestTimeout();
    jsonRequestFactory.setRequestTimeout(timeout);

    return new AutoCloseable() {

      @Override
      public void close() throws Exception {
        jsonRequestFactory.setRequestTimeout(old);
      }
    };
  }


}
