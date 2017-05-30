//-----------------------------------------------------------------------
// <copyright file="DocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Util;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Net.Http;
using System.Threading.Tasks;

namespace Raven.Client.Documents
{
    /// <summary>
    /// Manages access to RavenDB and open sessions to work with RavenDB.
    /// </summary>
    public class DocumentStore : DocumentStoreBase
    {
        private readonly AtomicDictionary<IDatabaseChanges> _databaseChanges = new AtomicDictionary<IDatabaseChanges>(StringComparer.OrdinalIgnoreCase);

        private readonly ConcurrentDictionary<string, EvictItemsFromCacheBasedOnChanges> _observeChangesAndEvictItemsFromCacheForDatabases = new ConcurrentDictionary<string, EvictItemsFromCacheBasedOnChanges>();

        private readonly ConcurrentDictionary<string, Lazy<RequestExecutor>> _requestExecutors = new ConcurrentDictionary<string, Lazy<RequestExecutor>>(StringComparer.OrdinalIgnoreCase);

        private AsyncMultiDatabaseHiLoIdGenerator _asyncMultiDbHiLo;

        private AdminOperationExecutor _adminOperationExecutor;

        private OperationExecutor _operationExecutor;

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
                if (Urls == null)
                    return null;
                if (Database != null)
                    return string.Join(",", Urls) + " (DB: " + Database + ")";
                return string.Join(",", Urls);
            }
            set => _identifier = value;
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
            if (options.Urls.Count > 0)
                Urls = options.Urls.ToArray();
            if (string.IsNullOrEmpty(options.Database) == false)
                Database = options.Database;
            if (string.IsNullOrEmpty(options.ApiKey) == false)
                ApiKey = options.ApiKey;

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
            foreach (var changes in _databaseChanges)
            {
                using (changes.Value) { }
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

            foreach (var kvp in _requestExecutors)
            {
                if (kvp.Value.IsValueCreated == false)
                    continue;

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
            var databaseName = options.Database ?? Database;
            var requestExecutor = GetRequestExecutor(databaseName);
            var session = new DocumentSession(databaseName, this, sessionId, requestExecutor);
            RegisterEvents(session);
            // AfterSessionCreated(session);
            return session;
        }

        public async Task ForceUpdateTopologyFor(string url, string databaseName = null, int timeout = 0)
        {
            var requestExecutor = GetRequestExecutor(databaseName);
            await requestExecutor.UpdateTopologyAsync(new ServerNode
            {
                Url = url,
                Database = databaseName
            }, timeout);
        }

        public override RequestExecutor GetRequestExecutor(string database = null)
        {
            if (database == null)
                database = Database;

            Lazy<RequestExecutor> lazy;
            if (_requestExecutors.TryGetValue(database, out lazy))
            {
                return lazy.Value;
            }
            
            lazy = new Lazy<RequestExecutor>(() => RequestExecutor.Create(Urls, database, ApiKey));

            lazy = _requestExecutors.GetOrAdd(database, lazy);

            return lazy.Value;
        }

        public override IDisposable SetRequestsTimeout(TimeSpan timeout, string database = null)
        {
            AssertInitialized();

            var requestExecutor = GetRequestExecutor(database);
            var oldTimeout = requestExecutor.DefaultTimeout;
            requestExecutor.DefaultTimeout = timeout;

            return new DisposableAction(() =>
            {
                requestExecutor.DefaultTimeout = oldTimeout;
            });
        }

        /// <summary>
        /// Initializes this instance.
        /// </summary>
        /// <returns></returns>
        public override IDocumentStore Initialize()
        {
            if (Initialized)
                return this;

            AssertValidConfiguration();

            try
            {
                if (Conventions.AsyncDocumentIdGenerator == null) // don't overwrite what the user is doing
                {
                    var generator = new AsyncMultiDatabaseHiLoIdGenerator(this, Conventions);
                    _asyncMultiDbHiLo = generator;
                    Conventions.AsyncDocumentIdGenerator = (dbName, entity) => generator.GenerateDocumentIdAsync(dbName, entity);
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

        /// <summary>
        /// validate the configuration for the document store
        /// </summary>
        protected virtual void AssertValidConfiguration()
        {
            if (Urls == null || Urls?.Length == 0)
            {
                throw new ArgumentException("Document store URLs cannot be empty", nameof(Urls));
            }
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
            var re = GetRequestExecutor(Database);
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

            return _databaseChanges.GetOrAdd(database ?? Database, CreateDatabaseChanges);
        }

        protected virtual IDatabaseChanges CreateDatabaseChanges(string database)
        {
            return new DatabaseChanges(GetRequestExecutor(database), Conventions, database, () => _databaseChanges.Remove(database));
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
            throw new NotImplementedException("This feature is not yet implemented");

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

        private AsyncDocumentSession OpenAsyncSessionInternal(SessionOptions options)
        {
            AssertInitialized();
            EnsureNotClosed();

            var sessionId = Guid.NewGuid();
            var databaseName = options.Database ?? Database;
            var requestExecutor = GetRequestExecutor(databaseName);
            var session = new AsyncDocumentSession(databaseName, this, requestExecutor, sessionId);
            //AfterSessionCreated(session);
            return session;
        }

        /// <summary>
        /// Opens the async session.
        /// </summary>
        /// <returns></returns>
        public override IAsyncDocumentSession OpenAsyncSession(string database)
        {
            return OpenAsyncSession(new SessionOptions
            {
                Database = database
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

        public override AdminOperationExecutor Admin => _adminOperationExecutor ?? (_adminOperationExecutor = new AdminOperationExecutor(this));

        public override OperationExecutor Operations => _operationExecutor ?? (_operationExecutor = new OperationExecutor(this));

        public override BulkInsertOperation BulkInsert(string database = null)
        {
            return new BulkInsertOperation(database ?? Database, this);
        }

        protected override void AfterSessionCreated(InMemoryDocumentSessionOperations session)
        {
            throw new NotImplementedException("This feature is not yet implemented");
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

        internal Task GetObserveChangesAndEvictItemsFromCacheTask(string database = null)
        {
            var changes = _observeChangesAndEvictItemsFromCacheForDatabases.GetOrDefault(database ?? Database);

            return changes == null ? Task.CompletedTask : changes.ConnectionTask;
        }
    }
}
