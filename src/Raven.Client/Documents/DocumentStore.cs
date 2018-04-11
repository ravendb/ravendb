//-----------------------------------------------------------------------
// <copyright file="DocumentStore.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------

using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Threading.Tasks;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Identity;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Http;
using Raven.Client.Util;

namespace Raven.Client.Documents
{
    /// <summary>
    /// Manages access to RavenDB and open sessions to work with RavenDB.
    /// </summary>
    public class DocumentStore : DocumentStoreBase
    {
        private readonly AtomicDictionary<IDatabaseChanges> _databaseChanges = new AtomicDictionary<IDatabaseChanges>(StringComparer.OrdinalIgnoreCase);

        private ConcurrentDictionary<string, Lazy<EvictItemsFromCacheBasedOnChanges>> _aggressiveCacheChanges = new ConcurrentDictionary<string, Lazy<EvictItemsFromCacheBasedOnChanges>>();

        private readonly ConcurrentDictionary<string, EvictItemsFromCacheBasedOnChanges> _observeChangesAndEvictItemsFromCacheForDatabases = new ConcurrentDictionary<string, EvictItemsFromCacheBasedOnChanges>();

        private readonly ConcurrentDictionary<string, Lazy<RequestExecutor>> _requestExecutors = new ConcurrentDictionary<string, Lazy<RequestExecutor>>(StringComparer.OrdinalIgnoreCase);

        private AsyncMultiDatabaseHiLoIdGenerator _asyncMultiDbHiLo;

        private MaintenanceOperationExecutor _maintenanceOperationExecutor;

        private OperationExecutor _operationExecutor;

        private DatabaseSmuggler _smuggler;

        private string _identifier;
        private bool _aggressiveCachingUsed;

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
        /// Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        public override void Dispose()
        {
            BeforeDispose?.Invoke(this, EventArgs.Empty);
#if DEBUG
            GC.SuppressFinalize(this);
#endif

            foreach (var observeChangesAndEvictItemsFromCacheForDatabase in _observeChangesAndEvictItemsFromCacheForDatabases)
                observeChangesAndEvictItemsFromCacheForDatabase.Value.Dispose();

            var tasks = new List<Task>();
            foreach (var changes in _databaseChanges)
            {
                using (changes.Value)
                { }
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
            AssertInitialized();
            EnsureNotClosed();

            var sessionId = Guid.NewGuid();
            var databaseName = options.Database ?? Database;
            var requestExecutor = options.RequestExecutor ?? GetRequestExecutor(databaseName);
            var session = new DocumentSession(databaseName, this, sessionId, requestExecutor);
            RegisterEvents(session);
            // AfterSessionCreated(session);
            return session;
        }

        public event EventHandler<RequestExecutor> RequestExecutorCreated;

        public override RequestExecutor GetRequestExecutor(string database = null)
        {
            AssertInitialized();

            if (database == null)
                database = Database;

            if (_requestExecutors.TryGetValue(database, out var lazy))
                return lazy.Value;

            RequestExecutor CreateRequestExecutor()
            {
                var requestExecutor = RequestExecutor.Create(Urls, database, Certificate, Conventions);
                RequestExecutorCreated?.Invoke(this, requestExecutor);
                return requestExecutor;
            }

            RequestExecutor CreateRequestExecutorForSingleNode()
            {
                var forSingleNode = RequestExecutor.CreateForSingleNodeWithConfigurationUpdates(Urls[0], database, Certificate, Conventions);
                RequestExecutorCreated?.Invoke(this, forSingleNode);
                return forSingleNode;
            }

            lazy = Conventions.DisableTopologyUpdates == false
                ? new Lazy<RequestExecutor>(CreateRequestExecutor)
                : new Lazy<RequestExecutor>(CreateRequestExecutorForSingleNode);

            lazy = _requestExecutors.GetOrAdd(database, lazy);

            return lazy.Value;
        }

        public override IDisposable SetRequestTimeout(TimeSpan timeout, string database = null)
        {
            AssertInitialized();

            database = (database ?? Database) ?? throw new InvalidOperationException("Cannot use SetRequestTimeout without a default database defined " +
                                                                                     "unless 'database' parameter is provided. Did you forget to pass 'database' parameter?");

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

                Conventions.Freeze();
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
            if (Urls == null || Urls.Length == 0)
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
        public override IDisposable DisableAggressiveCaching(string database = null)
        {
            AssertInitialized();
            database = (database ?? Database) ?? throw new InvalidOperationException("Cannot use DisableAggressiveCaching without a default database defined " +
                                                                                   "unless 'database' parameter is provided. Did you forget to pass 'database' parameter?");
            var re = GetRequestExecutor(database);
            var old = re.AggressiveCaching.Value;
            re.AggressiveCaching.Value = null;
            return new DisposableAction(() => re.AggressiveCaching.Value = old);
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
            return new DatabaseChanges(GetRequestExecutor(database), database, () => _databaseChanges.Remove(database));
        }

        public Exception GetLastDatabaseChangesStateException(string database = null)
        {
            if (_databaseChanges.TryGetValue(database ?? Database, out IDatabaseChanges databaseChanges))
            {
                return ((DatabaseChanges)databaseChanges).GetLastConnectionStateException();
            }

            return null;
        }

        /// <summary>
        /// Setup the context for aggressive caching.
        /// </summary>
        /// <remarks>
        /// Aggressive caching means that we will not check the server to see whether the response
        /// we provide is current or not, but will serve the information directly from the local cache
        /// without touching the server.
        /// </remarks>
        public override IDisposable AggressivelyCacheFor(TimeSpan cacheDuration, string database = null)
        {
            AssertInitialized();
            database = (database ?? Database) ?? throw new InvalidOperationException("Cannot use AggressivelyCache and AggressivelyCacheFor without a default database defined " +
                                                                                     "unless 'database' parameter is provided. Did you forget to pass 'database' parameter?");
            if (_aggressiveCachingUsed == false)
            {
                ListenToChangesAndUpdateTheCache(database);
            }
            var re = GetRequestExecutor(database);
            var old = re.AggressiveCaching.Value;
            re.AggressiveCaching.Value = new AggressiveCacheOptions
            {
                Duration = cacheDuration
            };
            return new DisposableAction(() => re.AggressiveCaching.Value = old);
        }

        private void ListenToChangesAndUpdateTheCache(string database)
        {
            Debug.Assert(database != null);
            // this is intentionally racy, most cases, we'll already 
            // have this set once, so we won't need to do it again
            _aggressiveCachingUsed = true;
            if (_aggressiveCacheChanges.TryGetValue(database, out var lazy) == false)
            {
                lazy = _aggressiveCacheChanges.GetOrAdd(database, new Lazy<EvictItemsFromCacheBasedOnChanges>(
                    () => new EvictItemsFromCacheBasedOnChanges(this, database)));
            }
            GC.KeepAlive(lazy.Value); // here we force it to be evaluated
        }

        private AsyncDocumentSession OpenAsyncSessionInternal(SessionOptions options)
        {
            AssertInitialized();
            EnsureNotClosed();

            var sessionId = Guid.NewGuid();
            var databaseName = options.Database ?? Database;
            var requestExecutor = options.RequestExecutor ?? GetRequestExecutor(databaseName);
            var session = new AsyncDocumentSession(databaseName, this, requestExecutor, sessionId);
            RegisterEvents(session);
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
        /// <summary>
        /// Called before dispose is completed
        /// </summary>
        public override event EventHandler BeforeDispose;

        public override DatabaseSmuggler Smuggler => _smuggler ?? (_smuggler = new DatabaseSmuggler(this));

        public override MaintenanceOperationExecutor Maintenance
        {
            get
            {
                AssertInitialized();
                return _maintenanceOperationExecutor ?? (_maintenanceOperationExecutor = new MaintenanceOperationExecutor(this));
            }
        }

        public override OperationExecutor Operations
        {
            get
            {
                AssertInitialized();
                return _operationExecutor ?? (_operationExecutor = new OperationExecutor(this));
            }
        }

        public override BulkInsertOperation BulkInsert(string database = null)
        {
            AssertInitialized();
            return new BulkInsertOperation(database ?? Database, this);
        }
    }
}
