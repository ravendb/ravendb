//-----------------------------------------------------------------------
// <copyright file="DocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Server;
using Raven.Client.Util;

namespace Raven.Client.Documents
{
    /// <summary>
    /// Manages access to RavenDB and open sessions to work with RavenDB.
    /// </summary>
    public class DocumentStore : DocumentStoreBase
    {
        private readonly AtomicDictionary<IDatabaseChanges> _databaseChanges = new AtomicDictionary<IDatabaseChanges>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<string, EvictItemsFromCacheBasedOnChanges> _observeChangesAndEvictItemsFromCacheForDatabases = new ConcurrentDictionary<string, EvictItemsFromCacheBasedOnChanges>();

        private readonly ConcurrentDictionary<string, Lazy<RequestExecutor>> _requestExecuters = new ConcurrentDictionary<string, Lazy<RequestExecutor>>(StringComparer.OrdinalIgnoreCase);

        private AsyncMultiDatabaseHiLoKeyGenerator _asyncMultiDbHiLo;

        private AdminOperationExecuter _adminOperationExecuter;

        private OperationExecuter _operationExecuter;

        private DatabaseSmuggler _smuggler;

        private string _identifier;

        /// <summary>
        /// Gets or sets the identifier for this store.
        /// </summary>
        /// <value>The identifier.</value>
        public override string Identifier
        {
            get
            {
                if (_identifier != null)
                    return _identifier;
                if (Url == null)
                    return null;
                if (DefaultDatabase != null)
                    return Url + " (DB: " + DefaultDatabase + ")";
                return Url;
            }
            set { _identifier = value; }
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
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
#if DEBUG
            GC.SuppressFinalize(this);
#endif

            foreach (var observeChangesAndEvictItemsFromCacheForDatabase in _observeChangesAndEvictItemsFromCacheForDatabases)
                observeChangesAndEvictItemsFromCacheForDatabase.Value.Dispose();

            var tasks = new List<Task>();
            foreach (var databaseChange in _databaseChanges)
            {
                //var remoteDatabaseChanges = databaseChange.Value as RemoteDatabaseChanges;
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
            if (_asyncMultiDbHiLo != null)
            {
                try
                {
                    AsyncHelpers.RunSync(() => _asyncMultiDbHiLo.ReturnUnusedRange());
                }
                catch
                {
                    // failed, because server is down.
                }
            }               

            Subscriptions?.Dispose();

            AsyncSubscriptions?.Dispose();

            WasDisposed = true;
            AfterDispose?.Invoke(this, EventArgs.Empty);

            foreach (var kvp in _requestExecuters)
            {
                if(kvp.Value.IsValueCreated == false)
                    continue;
                ;
                kvp.Value.Value.Dispose();
            }
        }

        /// <summary>
        /// Opens the session.
        /// </summary>
        /// <returns></returns>
        public override IDocumentSession OpenSession()
        {
            return OpenSession(new SessionOptions());
        }

        /// <summary>
        /// Opens the session for a particular database
        /// </summary>
        public override IDocumentSession OpenSession(string database)
        {
            return OpenSession(new SessionOptions
            {
                Database = database
            });
        }


        public override IDocumentSession OpenSession(SessionOptions options)
        {
            EnsureNotClosed();

            var sessionId = Guid.NewGuid();
            var databaseName = options.Database ?? DefaultDatabase ?? MultiDatabase.GetDatabaseName(Url);
            var requestExecuter = GetRequestExecuter(databaseName);
            var session = new DocumentSession(databaseName, this, sessionId, requestExecuter);
            RegisterEvents(session);
            // AfterSessionCreated(session);
            return session;
        }

        public async Task ForceUpdateTopologyFor(string databaseName = null)
        {
            var requestExecutor = GetRequestExecuter(databaseName);
            await requestExecutor.UpdateTopologyAsync();
        }

        public override RequestExecutor GetRequestExecuter(string databaseName = null)
        {
            if (databaseName == null)
                databaseName = DefaultDatabase;

            Lazy<RequestExecutor> lazy;
            if (_requestExecuters.TryGetValue(databaseName, out lazy))
                return lazy.Value;

            lazy = new Lazy<RequestExecutor>(() => RequestExecutor.Create(Url, databaseName, ApiKey));

            lazy = _requestExecuters.GetOrAdd(databaseName, lazy);

            return lazy.Value;
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
            if (Initialized)
                return this;

            AssertValidConfiguration();

            //jsonRequestFactory = new HttpJsonRequestFactory(MaxNumberOfCachedRequests, HttpMessageHandlerFactory, Conventions.AcceptGzipContent, Conventions.AuthenticationScheme);

            try
            {
                // TODO iftah
                //SecurityExtensions.InitializeSecurity(Conventions, jsonRequestFactory, Url, Credentials);

                if (Conventions.AsyncDocumentIdGenerator == null) // don't overwrite what the user is doing
                {
                    var generator = new AsyncMultiDatabaseHiLoKeyGenerator(this, Conventions);
                    _asyncMultiDbHiLo = generator;
                    Conventions.AsyncDocumentIdGenerator = (dbName, entity) => generator.GenerateDocumentKeyAsync(dbName, entity);
                }

                Initialized = true;
            }
            catch (Exception)
            {
                Dispose();
                throw;
            }

            return this;
        }

        public Task<string> Generate(string dbName, DocumentConventions conventions,
                                                     object entity)
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// validate the configuration for the document store
        /// </summary>
        protected virtual void AssertValidConfiguration()
        {
            if (string.IsNullOrEmpty(Url))
                throw new ArgumentException("Document store URL cannot be empty", nameof(Url));
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

            return _databaseChanges.GetOrAdd(database ?? DefaultDatabase, CreateDatabaseChanges);
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
        /// Aggressive caching means that we will not check the server to see whether the response
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

        private AsyncDocumentSession OpenAsyncSessionInternal(SessionOptions options)
        {
            AssertInitialized();
            EnsureNotClosed();

            var sessionId = Guid.NewGuid();
            var databaseName = options.Database ?? DefaultDatabase ?? MultiDatabase.GetDatabaseName(Url);
            var requestExecuter = GetRequestExecuter(databaseName);
            var session = new AsyncDocumentSession(databaseName, this, requestExecuter, sessionId);
            //AfterSessionCreated(session);
            return session;
        }

        /// <summary>
        /// Opens the async session.
        /// </summary>
        /// <returns></returns>
        public override IAsyncDocumentSession OpenAsyncSession(string databaseName)
        {
            return OpenAsyncSession(new SessionOptions
            {
                Database = databaseName
            });
        }

        public override IAsyncDocumentSession OpenAsyncSession(SessionOptions options)
        {
            return OpenAsyncSessionInternal(options);
        }

        public override IAsyncDocumentSession OpenAsyncSession()
        {
            return OpenAsyncSessionInternal(new SessionOptions());
        }

        /// <summary>
        /// Called after dispose is completed
        /// </summary>
        public override event EventHandler AfterDispose;

        public Func<HttpMessageHandler> HttpMessageHandlerFactory { get; set; }

        public DatabaseSmuggler Smuggler => _smuggler ?? (_smuggler = new DatabaseSmuggler(this));

        public override AdminOperationExecuter Admin => _adminOperationExecuter ?? (_adminOperationExecuter = new AdminOperationExecuter(this));

        public override OperationExecuter Operations => _operationExecuter ?? (_operationExecuter = new OperationExecuter(this));

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
            var databaseName = database ?? MultiDatabase.GetDatabaseName(Url) ?? Constants.Documents.SystemDatabase;
            var changes = _observeChangesAndEvictItemsFromCacheForDatabases.GetOrDefault(databaseName);

            return changes == null ? Task.CompletedTask : changes.ConnectionTask;
        }
    }
}
