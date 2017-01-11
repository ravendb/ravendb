//-----------------------------------------------------------------------
// <copyright file="DocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net;
using System.Net.Http;
using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Abstractions.Extensions;
using Raven.NewClient.Abstractions.Util;
using Raven.NewClient.Client.Changes;
using Raven.NewClient.Client.Extensions;

using System.Threading.Tasks;
using Raven.NewClient.Client.Document.Async;
using Raven.NewClient.Client.Http;
using Raven.NewClient.Client.Metrics;
using Raven.NewClient.Client.Smuggler;
using Raven.NewClient.Client.Util;
using Raven.NewClient.Operations;


namespace Raven.NewClient.Client.Document
{
    /// <summary>
    /// Manages access to RavenDB and open sessions to work with RavenDB.
    /// </summary>
    public class DocumentStore : DocumentStoreBase
    {
        //private readonly ConcurrentDictionary<string, IDocumentStoreReplicationInformer> replicationInformers = new ConcurrentDictionary<string, IDocumentStoreReplicationInformer>(StringComparer.OrdinalIgnoreCase);

        //private readonly ConcurrentDictionary<string, ClusterAwareRequestExecuter> clusterAwareRequestExecuters = new ConcurrentDictionary<string, ClusterAwareRequestExecuter>(StringComparer.OrdinalIgnoreCase);

        private readonly AtomicDictionary<IDatabaseChanges> databaseChanges = new AtomicDictionary<IDatabaseChanges>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<string, EvictItemsFromCacheBasedOnChanges> observeChangesAndEvictItemsFromCacheForDatabases = new ConcurrentDictionary<string, EvictItemsFromCacheBasedOnChanges>();

        private readonly ConcurrentDictionary<string, RequestTimeMetric> requestTimeMetrics = new ConcurrentDictionary<string, RequestTimeMetric>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<string, Lazy<RequestExecuter>> _requestExecuters = new ConcurrentDictionary<string, Lazy<RequestExecuter>>(StringComparer.OrdinalIgnoreCase);

        private AsyncMultiDatabaseHiLoKeyGenerator _asyncMultiDbHiLo;

        private AdminOperationExecuter _adminOperationExecuter;

        /// <summary>
        /// The current session id - only used during construction
        /// </summary>
        [ThreadStatic]
        private static Guid? currentSessionId;
        private const int DefaultNumberOfCachedRequests = 2048;
        private int maxNumberOfCachedRequests = DefaultNumberOfCachedRequests;
        private bool aggressiveCachingUsed;

        /// <summary>
        /// Whatever this instance has json request factory available
        /// </summary>
        public override bool HasJsonRequestFactory
        {
            get { return true; }
        }

        public ReplicationBehavior Replication { get; private set; }

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentStore"/> class.
        /// </summary>
        public DocumentStore()
        {
            Replication = new ReplicationBehavior(this);
            Credentials = CredentialCache.DefaultNetworkCredentials;
            SharedOperationsHeaders = new System.Collections.Specialized.NameValueCollection();
            Conventions = new DocumentConvention();
        }

        private string identifier;

        /// <summary>
        /// Gets or sets the credentials.
        /// </summary>
        /// <value>The credentials.</value>
        public ICredentials Credentials { get; set; }

        /// <summary>
        /// Gets or sets the identifier for this store.
        /// </summary>
        /// <value>The identifier.</value>
        public override string Identifier
        {
            get
            {
                if (identifier != null)
                    return identifier;
                if (Url == null)
                    return null;
                if (DefaultDatabase != null)
                    return Url + " (DB: " + DefaultDatabase + ")";
                return Url;
            }
            set { identifier = value; }
        }

        /// <summary>
        /// The API Key to use when authenticating against a RavenDB server that
        /// supports API Key authentication
        /// </summary>
        public string ApiKey { get; set; }

        /// <summary>
        /// Set document store settings based on a given connection string.
        /// </summary>
        /// <param name="connString">The connection string to parse</param>
        public void ParseConnectionString(string connString)
        {
            var connectionStringOptions = ConnectionStringParser<RavenConnectionStringOptions>.FromConnectionString(connString);
            connectionStringOptions.Parse();
            SetConnectionStringSettings(connectionStringOptions.ConnectionStringOptions);
        }

        /// <summary>
        /// Copy the relevant connection string settings
        /// </summary>
        protected virtual void SetConnectionStringSettings(RavenConnectionStringOptions options)
        {
            if (options.Credentials != null)
                Credentials = options.Credentials;
            if (string.IsNullOrEmpty(options.Url) == false)
                Url = options.Url;
            if (string.IsNullOrEmpty(options.DefaultDatabase) == false)
                DefaultDatabase = options.DefaultDatabase;
            if (string.IsNullOrEmpty(options.ApiKey) == false)
                ApiKey = options.ApiKey;
            if (options.FailoverServers != null)
                FailoverServers = options.FailoverServers;

        }

        /// <summary>
        /// Gets or sets the default database name.
        /// </summary>
        /// <value>The default database name.</value>
        public override string DefaultDatabase { get; set; }

        /// <summary>
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
#if DEBUG
            GC.SuppressFinalize(this);
#endif

            foreach (var observeChangesAndEvictItemsFromCacheForDatabase in observeChangesAndEvictItemsFromCacheForDatabases)
            {
                observeChangesAndEvictItemsFromCacheForDatabase.Value.Dispose();
            }

            var tasks = new List<Task>();
            foreach (var databaseChange in databaseChanges)
            {
                var remoteDatabaseChanges = databaseChange.Value as RemoteDatabaseChanges;
                //TODO iftah
                /*if (remoteDatabaseChanges != null)
                {
                    tasks.Add(remoteDatabaseChanges.DisposeAsync());
                }
                else
                {
                    using (databaseChange.Value as IDisposable) { }
                }*/
            }

            // try to wait until all the async disposables are completed
            Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(3));
            // if this is still going, we continue with disposal, it is for graceful shutdown only, anyway

            //return unused hilo keys
            AsyncHelpers.RunSync(() => _asyncMultiDbHiLo?.ReturnUnusedRange());

            Subscriptions?.Dispose();

            AsyncSubscriptions?.Dispose();

            WasDisposed = true;
            AfterDispose?.Invoke(this, EventArgs.Empty);
        }

        /// <summary>
        /// Opens the session.
        /// </summary>
        /// <returns></returns>
        public override IDocumentSession OpenSession()
        {
            return OpenSession(new OpenSessionOptions());
        }

        /// <summary>
        /// Opens the session for a particular database
        /// </summary>
        public override IDocumentSession OpenSession(string database)
        {
            return OpenSession(new OpenSessionOptions
            {
                Database = database
            });
        }


        public override IDocumentSession OpenSession(OpenSessionOptions options)
        {
            EnsureNotClosed();

            var sessionId = Guid.NewGuid();
            currentSessionId = sessionId;
            try
            {
                var databaseName = options.Database ?? DefaultDatabase ?? MultiDatabase.GetDatabaseName(Url);
                var requestExecuter = GetRequestExecuter(databaseName);
                var session = new DocumentSession(databaseName, this, sessionId, requestExecuter);
                RegisterEvents(session);
                // AfterSessionCreated(session);
                return session;
            }
            finally
            {
                currentSessionId = null;
            }
        }

        public override RequestExecuter GetRequestExecuter(string databaseName)
        {
            Lazy<RequestExecuter> lazy;
            if (_requestExecuters.TryGetValue(databaseName, out lazy))
                return lazy.Value;
            lazy = _requestExecuters.GetOrAdd(databaseName,
                dbName => new Lazy<RequestExecuter>(() => new RequestExecuter(Url, dbName, ApiKey)));
            return lazy.Value;
        }

        public override RequestExecuter GetRequestExecuterForDefaultDatabase()
        {
            return GetRequestExecuter(DefaultDatabase);
        }

        public override IDocumentStore Initialize()
        {
            return Initialize(true);
        }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        /// <returns></returns>
        public IDocumentStore Initialize(bool ensureDatabaseExists)
        {

            if (initialized)
                return this;

            AssertValidConfiguration();

            //jsonRequestFactory = new HttpJsonRequestFactory(MaxNumberOfCachedRequests, HttpMessageHandlerFactory, Conventions.AcceptGzipContent, Conventions.AuthenticationScheme);

            try
            {
                if (string.IsNullOrEmpty(ApiKey) == false)
                {
                    Credentials = null;
                }
                // TODO iftah
                //SecurityExtensions.InitializeSecurity(Conventions, jsonRequestFactory, Url, Credentials);

                InitializeInternal();

                if (Conventions.AsyncDocumentKeyGenerator == null) // don't overwrite what the user is doing
                {
                    var generator = new AsyncMultiDatabaseHiLoKeyGenerator(this, Conventions);
                    _asyncMultiDbHiLo = generator;
                    Conventions.AsyncDocumentKeyGenerator = (dbName, entity) => generator.GenerateDocumentKeyAsync(dbName, entity);
                }

                Smuggler = new DatabaseSmuggler(this);

                initialized = true;
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }

            return this;
        }

        public Task<string> Generate(string dbName, DocumentConvention conventions,
                                                     object entity)
        {
            throw new NotImplementedException();
        }

        public override void InitializeProfiling()
        {
            throw new NotImplementedException();

            /*if (jsonRequestFactory == null)
                throw new InvalidOperationException("Cannot call InitializeProfiling() before Initialize() was called.");
            Conventions.DisableProfiling = false;
            jsonRequestFactory.LogRequest += (sender, args) =>
            {
                if (Conventions.DisableProfiling)
                    return;
                if (args.TotalSize > 1024 * 1024 * 2)
                {
                    profilingContext.RecordAction(sender, new RequestResultArgs
                    {
                        Url = args.Url,
                        PostedData = "total request/response size > 2MB, not tracked",
                        Result = "total request/response size > 2MB, not tracked",
                    });
                    return;
                }
                profilingContext.RecordAction(sender, args);
            };*/
        }


        /// <summary>
        /// validate the configuration for the document store
        /// </summary>
        protected virtual void AssertValidConfiguration()
        {
            if (string.IsNullOrEmpty(Url))
                throw new ArgumentException("Document store URL cannot be empty", "Url");
        }


        /// <summary>
        /// Initialize the document store access method to RavenDB
        /// </summary>
        protected virtual void InitializeInternal()
        {
            /*var rootDatabaseUrl = MultiDatabase.GetRootDatabaseUrl(Url);

            //throw new NotImplementedException();
            /*databaseCommandsGenerator = () =>
            {
                string databaseUrl = Url;
                if (string.IsNullOrEmpty(DefaultDatabase) == false)
                {
                    databaseUrl = rootDatabaseUrl;
                    databaseUrl = databaseUrl + "/databases/" + DefaultDatabase;
                }
                return new ServerClient(new AsyncServerClient(databaseUrl, Conventions, new OperationCredentials(ApiKey, Credentials), jsonRequestFactory,
                     currentSessionId, GetRequestExecuterForDatabase, GetRequestTimeMetricForDatabase, Changes, null,
                     Listeners.ConflictListeners, true, Conventions.ClusterBehavior));
            };

            asyncDatabaseCommandsGenerator = () =>
            {
                var asyncServerClient = new AsyncServerClient(Url, Conventions, new OperationCredentials(ApiKey, Credentials), jsonRequestFactory, 
                    currentSessionId, GetRequestExecuterForDatabase, GetRequestTimeMetricForDatabase, Changes, null, 
                    Listeners.ConflictListeners, true, Conventions.ClusterBehavior);

                if (string.IsNullOrEmpty(DefaultDatabase))
                    return asyncServerClient;
                return asyncServerClient.ForDatabase(DefaultDatabase);
            };*/
        }

        /*public IDocumentStoreReplicationInformer GetReplicationInformerForDatabase(string dbName = null)
        {
            var key = Url;
            dbName = dbName ?? DefaultDatabase;
            if (string.IsNullOrEmpty(dbName) == false)
            {
                key = MultiDatabase.GetRootDatabaseUrl(Url) + "/databases/" + dbName;
            }

            var result = replicationInformers.GetOrAdd(key, url => Conventions.ReplicationInformerFactory(url, jsonRequestFactory));

            if (FailoverServers == null)
                return result;

            if (dbName == DefaultDatabase)
            {
                if (FailoverServers.IsSetForDefaultDatabase && result.FailoverServers == null)
                    result.FailoverServers = FailoverServers.ForDefaultDatabase;
            }
            else
            {
                if (FailoverServers.IsSetForDatabase(dbName) && result.FailoverServers == null)
                    result.FailoverServers = FailoverServers.GetForDatabase(dbName);
            }

            return result;
        }*/

        /*private IRequestExecuter GetRequestExecuterForDatabase(AsyncServerClient serverClient, string databaseName, ClusterBehavior clusterBehavior, bool incrementStrippingBase)
        {
            var key = Url;
            databaseName = databaseName ?? DefaultDatabase;
            if (string.IsNullOrEmpty(databaseName) == false)
                key = MultiDatabase.GetRootDatabaseUrl(Url) + "/databases/" + databaseName;

            IRequestExecuter requestExecuter;
            if (clusterBehavior == ClusterBehavior.None)
                requestExecuter = new ReplicationAwareRequestExecuter(replicationInformers.GetOrAdd(key, url => Conventions.ReplicationInformerFactory(url, jsonRequestFactory)), GetRequestTimeMetricForDatabase(databaseName));
            else
                requestExecuter = clusterAwareRequestExecuters.GetOrAdd(key, url => new ClusterAwareRequestExecuter());

            requestExecuter.GetReadStripingBase(incrementStrippingBase);

            if (FailoverServers == null)
                return requestExecuter;

            if (databaseName == DefaultDatabase)
            {
                if (FailoverServers.IsSetForDefaultDatabase && requestExecuter.FailoverServers == null)
                    requestExecuter.FailoverServers = FailoverServers.ForDefaultDatabase;
            }
            else
            {
                if (FailoverServers.IsSetForDatabase(databaseName) && requestExecuter.FailoverServers == null)
                    requestExecuter.FailoverServers = FailoverServers.GetForDatabase(databaseName);
            }

            return requestExecuter;
        }*/

        public RequestTimeMetric GetRequestTimeMetricForDatabase(string databaseName)
        {
            var key = Url;
            databaseName = databaseName ?? DefaultDatabase;
            if (string.IsNullOrEmpty(databaseName) == false)
                key = MultiDatabase.GetRootDatabaseUrl(Url) + "/databases/" + databaseName;

            return requestTimeMetrics.GetOrAdd(key, new RequestTimeMetric());
        }

        /// <summary>
        /// Setup the context for no aggressive caching
        /// </summary>
        /// <remarks>
        /// This is mainly useful for internal use inside RavenDB, when we are executing
        /// queries that have been marked with WaitForNonStaleResults, we temporarily disable
        /// aggressive caching.
        /// </remarks>
        public override IDisposable DisableAggressiveCaching()
        {
            //WIP
            AssertInitialized();
            var re = GetRequestExecuter(DefaultDatabase);
            if (re.AggressiveCaching.Value != null)
            {
                var old = re.AggressiveCaching.Value.Duration;
                re.AggressiveCaching.Value.Duration = null;
                return new DisposableAction(() => re.AggressiveCaching.Value.Duration = old);
            }
            return null;
        }

        /// <summary>
        /// Subscribe to change notifications from the server
        /// </summary>
        public override IDatabaseChanges Changes(string database = null)
        {
            AssertInitialized();

            return databaseChanges.GetOrAdd(database ?? DefaultDatabase, CreateDatabaseChanges);
        }

        protected virtual IDatabaseChanges CreateDatabaseChanges(string database)
        {
            throw new NotImplementedException();
            /* if (string.IsNullOrEmpty(Url))
                 throw new InvalidOperationException("Changes API requires usage of server/client");

             database = database ?? DefaultDatabase ?? MultiDatabase.GetDatabaseName(Url);

             var dbUrl = MultiDatabase.GetRootDatabaseUrl(Url);
             if (string.IsNullOrEmpty(database) == false)
                 dbUrl = dbUrl + "/databases/" + database;

             using (NoSynchronizationContext.Scope())
             {
                 return new RemoteDatabaseChanges(dbUrl,
                     ApiKey,
                     Credentials,
                     Conventions,
                     () => databaseChanges.Remove(database),
                     (key, etag, conflictIds, metadata) => ((AsyncServerClient) AsyncDatabaseCommands).TryResolveConflictByUsingRegisteredListenersAsync(key, etag, conflictIds, metadata));
             }*/
        }

        /// <summary>
        /// Setup the context for aggressive caching.
        /// </summary>
        /// <param name="cacheDuration">Specify the aggressive cache duration</param>
        /// <remarks>
        /// Aggressive caching means that we will not check the server to see whatever the response
        /// we provide is current or not, but will serve the information directly from the local cache
        /// without touching the server.
        /// </remarks>
        public override IDisposable AggressivelyCacheFor(TimeSpan cacheDuration)
        {
            throw new NotImplementedException();

            /*AssertInitialized();

            if (cacheDuration.TotalSeconds < 1)
                throw new ArgumentException("cacheDuration must be longer than a single second");

            var old = jsonRequestFactory.AggressiveCacheDuration;
            jsonRequestFactory.AggressiveCacheDuration = cacheDuration;

            aggressiveCachingUsed = true;

            return new DisposableAction(() =>
            {
                jsonRequestFactory.AggressiveCacheDuration = old;
            });*/
        }

        /// <summary>
        /// Setup the WebRequest timeout for the session
        /// </summary>
        /// <param name="timeout">Specify the timeout duration</param>
        /// <remarks>
        /// Sets the timeout for the JsonRequest.  Scoped to the Current Thread.
        /// </remarks>
        public override IDisposable SetRequestsTimeoutFor(TimeSpan timeout)
        {
            throw new NotImplementedException();

            /*AssertInitialized();

            var old = jsonRequestFactory.RequestTimeout;
            jsonRequestFactory.RequestTimeout = timeout;

            return new DisposableAction(() =>
            {
                jsonRequestFactory.RequestTimeout = old;
            });*/
        }

        private AsyncDocumentSession OpenAsyncSessionInternal(OpenSessionOptions options)
        {
            AssertInitialized();
            EnsureNotClosed();

            var sessionId = Guid.NewGuid();
            currentSessionId = sessionId;
            try
            {
                var databaseName = options.Database ?? DefaultDatabase ?? MultiDatabase.GetDatabaseName(Url);
                var requestExecuter = GetRequestExecuter(databaseName);
                var session = new AsyncDocumentSession(databaseName, this, requestExecuter, sessionId);
                //AfterSessionCreated(session);
                return session;
            }
            finally
            {
                currentSessionId = null;
            }
        }

        /// <summary>
        /// Opens the async session.
        /// </summary>
        /// <returns></returns>
        public override IAsyncDocumentSession OpenAsyncSession(string databaseName)
        {
            return OpenAsyncSession(new OpenSessionOptions
            {
                Database = databaseName
            });
        }

        public override IAsyncDocumentSession OpenAsyncSession(OpenSessionOptions options)
        {
            return OpenAsyncSessionInternal(options);
        }

        public override IAsyncDocumentSession OpenAsyncSession()
        {
            return OpenAsyncSessionInternal(new OpenSessionOptions());
        }




        /// <summary>
        /// Called after dispose is completed
        /// </summary>
        public override event EventHandler AfterDispose;

        /// <summary>
        /// Max number of cached requests (default: 2048)
        /// </summary>
        /*public int MaxNumberOfCachedRequests
        {
            get { return maxNumberOfCachedRequests; }
            set
            {
                maxNumberOfCachedRequests = value;
                if (initialized == true)
                jsonRequestFactory.ResetCache(maxNumberOfCachedRequests);
            }
        }*/

        public Func<HttpMessageHandler> HttpMessageHandlerFactory { get; set; }

        public DatabaseSmuggler Smuggler { get; private set; }

        public AdminOperationExecuter Admin => _adminOperationExecuter ?? (_adminOperationExecuter = new AdminOperationExecuter(GetRequestExecuterForDefaultDatabase()));

        public override BulkInsertOperation BulkInsert(string database = null)
        {
            return new BulkInsertOperation(database ?? DefaultDatabase, this);
        }

        protected override void AfterSessionCreated(InMemoryDocumentSessionOperations session)
        {
            throw new NotImplementedException();
            /*if (Conventions.ShouldAggressiveCacheTrackChanges && aggressiveCachingUsed)
            {
                var databaseName = session.DatabaseName ?? Constants.SystemDatabase;
                observeChangesAndEvictItemsFromCacheForDatabases.GetOrAdd(databaseName,
                    _ => new EvictItemsFromCacheBasedOnChanges(databaseName,
                        Changes(databaseName),
                        jsonRequestFactory.ExpireItemsFromCache));
            }

            base.AfterSessionCreated(session);*/
        }

        public Task GetObserveChangesAndEvictItemsFromCacheTask(string database = null)
        {
            var databaseName = database ?? MultiDatabase.GetDatabaseName(Url) ?? Constants.SystemDatabase;
            var changes = observeChangesAndEvictItemsFromCacheForDatabases.GetOrDefault(databaseName);

            return changes == null ? new CompletedTask() : changes.ConnectionTask;
        }
    }
}
