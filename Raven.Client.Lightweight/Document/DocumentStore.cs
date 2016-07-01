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

using Raven.Abstractions.Cluster;
using Raven.Abstractions.Connection;
using Raven.Abstractions.Data;
using Raven.Abstractions.Extensions;
using Raven.Abstractions.Util;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Connection.Profiling;
using Raven.Client.Connection.Request;
using Raven.Client.Extensions;
using Raven.Client.Connection.Async;
using System.Threading.Tasks;
using Raven.Abstractions.Replication;
using Raven.Client.Document.Async;
using Raven.Client.Metrics;
using Raven.Client.Util;
using System.Threading;

#if !DNXCORE50
using Raven.Client.Document.DTC;
#endif

namespace Raven.Client.Document
{
    /// <summary>
    /// Manages access to RavenDB and open sessions to work with RavenDB.
    /// </summary>
    public class DocumentStore : DocumentStoreBase
    {
        private readonly ConcurrentDictionary<string, IDocumentStoreReplicationInformer> replicationInformers = new ConcurrentDictionary<string, IDocumentStoreReplicationInformer>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<string, ClusterAwareRequestExecuter> clusterAwareRequestExecuters = new ConcurrentDictionary<string, ClusterAwareRequestExecuter>(StringComparer.OrdinalIgnoreCase);

        private readonly AtomicDictionary<IDatabaseChanges> databaseChanges = new AtomicDictionary<IDatabaseChanges>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<string, EvictItemsFromCacheBasedOnChanges> observeChangesAndEvictItemsFromCacheForDatabases = new ConcurrentDictionary<string, EvictItemsFromCacheBasedOnChanges>();

        private readonly ConcurrentDictionary<string, RequestTimeMetric> requestTimeMetrics = new ConcurrentDictionary<string, RequestTimeMetric>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<string, bool> _dtcSupport = new ConcurrentDictionary<string, bool>(StringComparer.OrdinalIgnoreCase);

        /// <summary>
        /// The current session id - only used during construction
        /// </summary>
        [ThreadStatic]
        private static Guid? currentSessionId;
        private const int DefaultNumberOfCachedRequests = 2048;
        private int maxNumberOfCachedRequests = DefaultNumberOfCachedRequests;
        private bool aggressiveCachingUsed;

        /// <summary>
        /// Generate new instance of database commands
        /// </summary>
        protected Func<IDatabaseCommands> databaseCommandsGenerator;

        private HttpJsonRequestFactory jsonRequestFactory;

        /// <summary>
        /// Whatever this instance has json request factory available
        /// </summary>
        public override bool HasJsonRequestFactory
        {
            get { return true; }
        }

        public ReplicationBehavior Replication { get; private set; }

        ///<summary>
        /// Get the <see cref="HttpJsonRequestFactory"/> for the stores
        ///</summary>
        public override HttpJsonRequestFactory JsonRequestFactory
        {
            get
            {
                return jsonRequestFactory;
            }
        }

        /// <summary>
        /// Gets the database commands.
        /// </summary>
        /// <value>The database commands.</value>
        public override IDatabaseCommands DatabaseCommands
        {
            get
            {
                AssertInitialized();
                var commands = databaseCommandsGenerator();
                foreach (string key in SharedOperationsHeaders)
                {
                    var values = SharedOperationsHeaders.GetValues(key);
                    if (values == null)
                        continue;
                    foreach (var value in values)
                    {
                        commands.OperationsHeaders[key] = value;
                    }
                }
                return commands;
            }
        }

        protected Func<IAsyncDatabaseCommands> asyncDatabaseCommandsGenerator;
        /// <summary>
        /// Gets the async database commands.
        /// </summary>
        /// <value>The async database commands.</value>
        public override IAsyncDatabaseCommands AsyncDatabaseCommands
        {
            get
            {
                if (asyncDatabaseCommandsGenerator == null)
                    return null;
                return asyncDatabaseCommandsGenerator();
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="DocumentStore"/> class.
        /// </summary>
        public DocumentStore()
        {
            Replication = new ReplicationBehavior(this);
            Credentials = CredentialCache.DefaultNetworkCredentials;
            ResourceManagerId = new Guid("E749BAA6-6F76-4EEF-A069-40A4378954F8");
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

        private string connectionStringName;

        /// <summary>
        /// Gets or sets the name of the connection string name.
        /// </summary>
        public string ConnectionStringName
        {
            get { return connectionStringName; }
            set
            {
                connectionStringName = value;
                SetConnectionStringSettings(GetConnectionStringOptions());
            }
        }

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
            if (options.ResourceManagerId != Guid.Empty)
                ResourceManagerId = options.ResourceManagerId;
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

#if !DNXCORE50
            EnlistInDistributedTransactions = options.EnlistInDistributedTransactions;
#endif
        }

        /// <summary>
        /// Create the connection string parser
        /// </summary>
        protected virtual RavenConnectionStringOptions GetConnectionStringOptions()
        {
            var connectionStringOptions = ConnectionStringParser<RavenConnectionStringOptions>.FromConnectionStringName(connectionStringName);
            connectionStringOptions.Parse();
            return connectionStringOptions.ConnectionStringOptions;
        }

        /// <summary>
        /// Gets or sets the default database name.
        /// </summary>
        /// <value>The default database name.</value>
        public string DefaultDatabase { get; set; }

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
                if (remoteDatabaseChanges != null)
                {
                    tasks.Add(remoteDatabaseChanges.DisposeAsync());
                }
                else
                {
                    using (databaseChange.Value as IDisposable) { }
                }
            }

            foreach (var replicationInformer in replicationInformers)
            {
                replicationInformer.Value.Dispose();
            }

            // try to wait until all the async disposables are completed
            Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(3));

            if (Subscriptions != null)
                Subscriptions.Dispose();

            if (AsyncSubscriptions != null)
                AsyncSubscriptions.Dispose();

            // if this is still going, we continue with disposal, it is for grace only, anyway

            if (jsonRequestFactory != null)
                jsonRequestFactory.Dispose();

            WasDisposed = true;
            var afterDispose = AfterDispose;
            if (afterDispose != null)
                afterDispose(this, EventArgs.Empty);
        }

#if !DNXCORE50
        private ServicePoint rootServicePoint;
#endif

#if DEBUG
#if !DNXCORE50
        private readonly System.Diagnostics.StackTrace e = new System.Diagnostics.StackTrace();
#else
        private readonly string e = Environment.StackTrace;
#endif


        ~DocumentStore()
        {
            var buffer = e.ToString();
            var stacktraceDebug = string.Format("StackTrace of un-disposed document store recorded. Please make sure to dispose any document store in the tests in order to avoid race conditions in tests.{0}{1}{0}{0}", Environment.NewLine, buffer);
            Console.WriteLine(stacktraceDebug);
        }
#endif

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
                var session = new DocumentSession(options.Database, this, Listeners, sessionId,
                    SetupCommands(DatabaseCommands, options.Database, options.Credentials, options))
                {
                    DatabaseName = options.Database ?? DefaultDatabase ?? MultiDatabase.GetDatabaseName(Url)
                };
                AfterSessionCreated(session);
                return session;
            }
            finally
            {
                currentSessionId = null;
            }
        }

        private static IDatabaseCommands SetupCommands(IDatabaseCommands databaseCommands, string database, ICredentials credentialsForSession, OpenSessionOptions options)
        {
            if (database != null)
                databaseCommands = databaseCommands.ForDatabase(database);
            if (credentialsForSession != null)
                databaseCommands = databaseCommands.With(credentialsForSession);
            if (options.ForceReadFromMaster)
                databaseCommands.ForceReadFromMaster();
            return databaseCommands;
        }

        private static IAsyncDatabaseCommands SetupCommandsAsync(IAsyncDatabaseCommands databaseCommands, string database, ICredentials credentialsForSession, OpenSessionOptions options)
        {
            if (database != null)
                databaseCommands = databaseCommands.ForDatabase(database);
            if (credentialsForSession != null)
                databaseCommands = databaseCommands.With(credentialsForSession);
            if (options.ForceReadFromMaster)
                databaseCommands.ForceReadFromMaster();
            return databaseCommands;
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

            jsonRequestFactory = new HttpJsonRequestFactory(MaxNumberOfCachedRequests, HttpMessageHandlerFactory, Conventions.AcceptGzipContent, Conventions.AuthenticationScheme);

            try
            {
                SecurityExtensions.InitializeSecurity(Conventions, jsonRequestFactory, Url);
                InitializeInternal();

                if (Conventions.DocumentKeyGenerator == null)// don't overwrite what the user is doing
                {
                    var generator = new MultiDatabaseHiLoGenerator(32);
                    Conventions.DocumentKeyGenerator = (dbName, databaseCommands, entity) => generator.GenerateDocumentKey(dbName, databaseCommands, Conventions, entity);
                }

                if (Conventions.AsyncDocumentKeyGenerator == null && asyncDatabaseCommandsGenerator != null)
                {
                    var generator = new AsyncMultiDatabaseHiLoKeyGenerator(32);
                    Conventions.AsyncDocumentKeyGenerator = (dbName, commands, entity) => generator.GenerateDocumentKeyAsync(dbName, commands, Conventions, entity);
                }

                initialized = true;

#if !(MONO || DNXCORE50)
                RecoverPendingTransactions();
#endif                
                if (ensureDatabaseExists &&
                    string.IsNullOrEmpty(DefaultDatabase) == false &&
                    DefaultDatabase.Equals(Constants.SystemDatabase) == false) //system database exists anyway                    
                {
                    //If we have indication that the database is part of a replication cluster we don't want to create it,
                    //the reason for this is that we want the client to failover to a diffrent database.
                    var serverHash = ServerHash.GetServerHash(DatabaseCommands.Url);
                    var document = ReplicationInformerLocalCache.TryLoadReplicationInformationFromLocalCache(serverHash);
                    var replicationDocument = document?.DataAsJson.JsonDeserialization<ReplicationDocumentWithClusterInformation>();
                    if (replicationDocument == null)
                    {
                        DatabaseCommands.ForSystemDatabase().GlobalAdmin.EnsureDatabaseExists(DefaultDatabase, true);
                    }
                }
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }

            return this;
        }

        public override void InitializeProfiling()
        {
            if (jsonRequestFactory == null)
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
            };
        }

        public override bool CanEnlistInDistributedTransactions(string dbName)
        {
            return _dtcSupport.GetOrAdd(dbName, db => DatabaseCommands.ForDatabase(db).GetStatistics().SupportsDtc);
        }

#if !DNXCORE50
        private void RecoverPendingTransactions()
        {
            if (EnlistInDistributedTransactions == false)
                return;

            var pendingTransactionRecovery = new PendingTransactionRecovery(this);
            pendingTransactionRecovery.Execute(ResourceManagerId, DatabaseCommands);
        }
#endif

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
            var rootDatabaseUrl = MultiDatabase.GetRootDatabaseUrl(Url);

#if !DNXCORE50
            // TODO [ppekrol] how to set this?
            rootServicePoint = ServicePointManager.FindServicePoint(new Uri(rootDatabaseUrl));
            rootServicePoint.UseNagleAlgorithm = false;
            rootServicePoint.Expect100Continue = false;
            rootServicePoint.ConnectionLimit = 256;
            rootServicePoint.MaxIdleTime = Timeout.Infinite;
#endif

            databaseCommandsGenerator = () =>
            {
                var asyncServerClient = new AsyncServerClient(Url, Conventions, new OperationCredentials(ApiKey, Credentials), jsonRequestFactory,
                   currentSessionId, GetRequestExecuterForDatabase, GetRequestTimeMetricForUrl, null,
                   Listeners.ConflictListeners, true);

                var serverClient = new ServerClient(asyncServerClient);

                if (string.IsNullOrEmpty(DefaultDatabase))
                    return serverClient;
                return serverClient.ForDatabase(DefaultDatabase);
            };

            asyncDatabaseCommandsGenerator = () =>
            {
                var asyncServerClient = new AsyncServerClient(Url, Conventions, new OperationCredentials(ApiKey, Credentials), jsonRequestFactory,
                    currentSessionId, GetRequestExecuterForDatabase, GetRequestTimeMetricForUrl, null,
                    Listeners.ConflictListeners, true);

                if (string.IsNullOrEmpty(DefaultDatabase))
                    return asyncServerClient;
                return asyncServerClient.ForDatabase(DefaultDatabase);
            };
        }

        public IDocumentStoreReplicationInformer GetReplicationInformerForDatabase(string dbName = null)
        {
            var key = Url;
            dbName = dbName ?? DefaultDatabase;
            if (string.IsNullOrEmpty(dbName) == false)
            {
                key = MultiDatabase.GetRootDatabaseUrl(Url) + "/databases/" + dbName;
            }

            var result = replicationInformers.GetOrAdd(key, url => Conventions.ReplicationInformerFactory(url, jsonRequestFactory, GetRequestTimeMetricForUrl));

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
        }

        private IRequestExecuter GetRequestExecuterForDatabase(AsyncServerClient serverClient, string databaseName, bool incrementStrippingBase)
        {
            var key = Url;
            if (string.IsNullOrEmpty(databaseName) == false)
                key = MultiDatabase.GetRootDatabaseUrl(Url) + "/databases/" + databaseName;

            IRequestExecuter requestExecuter;
            
            if (Conventions.FailoverBehavior == FailoverBehavior.ReadFromLeaderWriteToLeader
                || Conventions.FailoverBehavior == FailoverBehavior.ReadFromLeaderWriteToLeaderWithFailovers
                || Conventions.FailoverBehavior == FailoverBehavior.ReadFromAllWriteToLeader
                || Conventions.FailoverBehavior == FailoverBehavior.ReadFromAllWriteToLeaderWithFailovers)
                requestExecuter = clusterAwareRequestExecuters.GetOrAdd(key, url => new ClusterAwareRequestExecuter());
            else
                requestExecuter = new ReplicationAwareRequestExecuter(replicationInformers.GetOrAdd(key, url => Conventions.ReplicationInformerFactory(url, jsonRequestFactory, GetRequestTimeMetricForUrl)));
            

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
        }

        public RequestTimeMetric GetRequestTimeMetricForUrl(string url)
        {
            if (url == null)
                throw new ArgumentNullException(nameof(url));

            return requestTimeMetrics.GetOrAdd(url, new RequestTimeMetric());
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
            AssertInitialized();

            var old = jsonRequestFactory.AggressiveCacheDuration;
            jsonRequestFactory.AggressiveCacheDuration = null;
            return new DisposableAction(() => jsonRequestFactory.AggressiveCacheDuration = old);
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
            if (string.IsNullOrEmpty(Url))
                throw new InvalidOperationException("Changes API requires usage of server/client");

            database = database ?? DefaultDatabase ?? MultiDatabase.GetDatabaseName(Url);

            var dbUrl = MultiDatabase.GetRootDatabaseUrl(Url);
            if (string.IsNullOrEmpty(database) == false &&
                string.Equals(database, Constants.SystemDatabase, StringComparison.OrdinalIgnoreCase) == false)
                dbUrl = dbUrl + "/databases/" + database;

            using (NoSynchronizationContext.Scope())
            {
                return new RemoteDatabaseChanges(dbUrl,
                        ApiKey,
                    Credentials,
                    jsonRequestFactory,
                    Conventions,
                    () => databaseChanges.Remove(database),
                    (key, etag, conflictIds, metadata) => ((AsyncServerClient)AsyncDatabaseCommands).TryResolveConflictByUsingRegisteredListenersAsync(key, etag, conflictIds, metadata));
            }
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
            AssertInitialized();

            if (cacheDuration.TotalSeconds < 1)
                throw new ArgumentException("cacheDuration must be longer than a single second");

            var old = jsonRequestFactory.AggressiveCacheDuration;
            jsonRequestFactory.AggressiveCacheDuration = cacheDuration;

            aggressiveCachingUsed = true;

            return new DisposableAction(() =>
            {
                jsonRequestFactory.AggressiveCacheDuration = old;
            });
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
            AssertInitialized();

            var old = jsonRequestFactory.RequestTimeout;
            jsonRequestFactory.RequestTimeout = timeout;

            return new DisposableAction(() =>
            {
                jsonRequestFactory.RequestTimeout = old;
            });
        }

        private IAsyncDocumentSession OpenAsyncSessionInternal(OpenSessionOptions options)
        {
            AssertInitialized();
            EnsureNotClosed();

            var sessionId = Guid.NewGuid();
            currentSessionId = sessionId;
            try
            {
                var asyncDatabaseCommands = SetupCommandsAsync(AsyncDatabaseCommands, options.Database, options.Credentials, options);
                if (AsyncDatabaseCommands == null)
                    throw new InvalidOperationException("You cannot open an async session because it is not supported on embedded mode");

                var session = new AsyncDocumentSession(options.Database, this, asyncDatabaseCommands, Listeners, sessionId)
                {
                    DatabaseName = options.Database ?? DefaultDatabase ?? MultiDatabase.GetDatabaseName(Url)
                };
                AfterSessionCreated(session);
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
        public override IAsyncDocumentSession OpenAsyncSession()
        {
            return OpenAsyncSession(new OpenSessionOptions());
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

        /// <summary>
        /// Called after dispose is completed
        /// </summary>
        public override event EventHandler AfterDispose;

        /// <summary>
        /// Max number of cached requests (default: 2048)
        /// </summary>
        public int MaxNumberOfCachedRequests
        {
            get { return maxNumberOfCachedRequests; }
            set
            {
                maxNumberOfCachedRequests = value;
                if (initialized == true)
                    jsonRequestFactory.ResetCache(maxNumberOfCachedRequests);
            }
        }

        public Func<HttpMessageHandler> HttpMessageHandlerFactory { get; set; }

        public override BulkInsertOperation BulkInsert(string database = null, BulkInsertOptions options = null)
        {
            return new BulkInsertOperation(database ?? DefaultDatabase, this, Listeners, options ?? new BulkInsertOptions(), Changes(database ?? DefaultDatabase));
        }

        protected override void AfterSessionCreated(InMemoryDocumentSessionOperations session)
        {
            if (Conventions.ShouldAggressiveCacheTrackChanges && aggressiveCachingUsed)
            {
                var databaseName = session.DatabaseName ?? Constants.SystemDatabase;
                observeChangesAndEvictItemsFromCacheForDatabases.GetOrAdd(databaseName,
                    _ => new EvictItemsFromCacheBasedOnChanges(databaseName,
                        Changes(databaseName),
                        jsonRequestFactory.ExpireItemsFromCache));
            }

            base.AfterSessionCreated(session);
        }

        public Task GetObserveChangesAndEvictItemsFromCacheTask(string database = null)
        {
            var databaseName = database ?? MultiDatabase.GetDatabaseName(Url) ?? Constants.SystemDatabase;
            var changes = observeChangesAndEvictItemsFromCacheForDatabases.GetOrDefault(databaseName);

            return changes == null ? new CompletedTask() : changes.ConnectionTask;
        }
    }
}

