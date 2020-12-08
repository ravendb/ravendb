using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Security.Cryptography.X509Certificates;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Documents.Changes;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Smuggler;
using Raven.Client.Documents.Subscriptions;
using Raven.Client.Documents.TimeSeries;
using Raven.Client.Extensions;
using Raven.Client.Http;
using Raven.Client.Util;

namespace Raven.Client.Documents
{
    /// <summary>
    /// Contains implementation of some IDocumentStore operations shared by DocumentStore implementations
    /// </summary>
    public abstract class DocumentStoreBase : IDocumentStore
    {
        protected DocumentStoreBase()
        {
            Subscriptions = new DocumentSubscriptions(this);
        }

        public abstract void Dispose();

        public abstract event EventHandler AfterDispose;

        public abstract event EventHandler BeforeDispose;

        /// <summary>
        /// Whether the instance has been disposed
        /// </summary>
        public bool WasDisposed { get; protected set; }

        /// <summary>
        /// Subscribe to change notifications from the server
        /// </summary>
        public abstract IDatabaseChanges Changes(string database = null);

        /// <inheritdoc />
        public abstract IDatabaseChanges Changes(string database, string nodeTag);

        public abstract IDisposable AggressivelyCacheFor(TimeSpan cacheDuration, string database = null);

        public abstract IDisposable AggressivelyCacheFor(TimeSpan cacheDuration, AggressiveCacheMode mode, string database = null);

        public abstract IDisposable DisableAggressiveCaching(string database = null);

        public abstract string Identifier { get; set; }

        public abstract IDocumentStore Initialize();

        public abstract IAsyncDocumentSession OpenAsyncSession();

        public abstract IAsyncDocumentSession OpenAsyncSession(string database);

        public abstract IAsyncDocumentSession OpenAsyncSession(SessionOptions sessionOptions);

        public abstract IDocumentSession OpenSession();

        public abstract IDocumentSession OpenSession(string database);

        public abstract IDocumentSession OpenSession(SessionOptions sessionOptions);

        /// <inheritdoc />
        public void ExecuteIndex(IAbstractIndexCreationTask task, string database = null)
        {
            AsyncHelpers.RunSync(() => ExecuteIndexAsync(task, database));
        }

        /// <inheritdoc />
        public Task ExecuteIndexAsync(IAbstractIndexCreationTask task, string database = null, CancellationToken token = default)
        {
            AssertInitialized();
            return task.ExecuteAsync(this, Conventions, database, token);
        }

        /// <inheritdoc />
        public void ExecuteIndexes(IEnumerable<IAbstractIndexCreationTask> tasks, string database = null)
        {
            AsyncHelpers.RunSync(() => ExecuteIndexesAsync(tasks, database));
        }

        /// <inheritdoc />
        public Task ExecuteIndexesAsync(IEnumerable<IAbstractIndexCreationTask> tasks, string database = null, CancellationToken token = default)
        {
            AssertInitialized();
            var indexesToAdd = IndexCreation.CreateIndexesToAdd(tasks, Conventions);

            database = this.GetDatabase(database);

            return Maintenance.ForDatabase(database).SendAsync(new PutIndexesOperation(indexesToAdd), token);
        }

        private TimeSeriesOperations _timeSeriesOperation;
        public TimeSeriesOperations TimeSeries => _timeSeriesOperation ??= new TimeSeriesOperations(this);

        private DocumentConventions _conventions;

        /// <summary>
        /// Gets the conventions.
        /// </summary>
        /// <value>The conventions.</value>
        public virtual DocumentConventions Conventions
        {
            get => _conventions ?? (_conventions = new DocumentConventions());
            set
            {
                AssertNotInitialized(nameof(Conventions));

                _conventions = value;
            }
        }

        /// <summary>
        /// Gets or sets the URLs.
        /// </summary>
        private string[] _urls = Array.Empty<string>();

        /// <summary>
        /// Gets or sets the Urls
        /// </summary>
        public string[] Urls
        {
            get => _urls;
            set
            {
                AssertNotInitialized(nameof(Urls));

                if (value == null)
                    throw new ArgumentNullException(nameof(value));

                for (var i = 0; i < value.Length; i++)
                {
                    if (value[i] == null)
                        throw new ArgumentNullException(nameof(value), "Urls cannot contain null");

                    if (Uri.TryCreate(value[i], UriKind.Absolute, out _) == false)
                        throw new ArgumentException($"'{value[i]}' is not a valid url");

                    value[i] = value[i].TrimEnd('/');
                }

                _urls = value;
            }
        }

        protected bool Initialized;
        private X509Certificate2 _certificate;

        private string _database;

        public abstract BulkInsertOperation BulkInsert(string database = null, CancellationToken token = default);

        public DocumentSubscriptions Subscriptions { get; }

        private readonly ConcurrentDictionary<string, long?> _lastRaftIndexPerDatabase = new ConcurrentDictionary<string, long?>(StringComparer.OrdinalIgnoreCase);

        internal long? GetLastTransactionIndex(string database)
        {
            if (_lastRaftIndexPerDatabase.TryGetValue(database, out var index) == false)
                return null;
            if (index == 0)
                return null;
            return index;
        }

        internal void SetLastTransactionIndex(string database, long? index)
        {
            if (index.HasValue == false)
                return;

            _lastRaftIndexPerDatabase.AddOrUpdate(database, _ => index, (_, initialValue) =>
            {
                if (initialValue >= index.Value)
                    return initialValue;
                return index.Value;
            });
        }

        protected void EnsureNotClosed()
        {
            if (WasDisposed)
                throw new ObjectDisposedException(GetType().Name, "The document store has already been disposed and cannot be used");
        }

        protected internal void AssertInitialized()
        {
            if (Initialized == false)
                throw new InvalidOperationException("You cannot open a session or access the database commands before initializing the document store. Did you forget calling Initialize()?");
        }

        private void AssertNotInitialized(string property)
        {
            if (Initialized)
                throw new InvalidOperationException($"You cannot set '{property}' after the document store has been initialized.");
        }

        public event EventHandler<BeforeStoreEventArgs> OnBeforeStore;

        public event EventHandler<AfterSaveChangesEventArgs> OnAfterSaveChanges;

        public event EventHandler<BeforeDeleteEventArgs> OnBeforeDelete;

        public event EventHandler<BeforeQueryEventArgs> OnBeforeQuery;

        public event EventHandler<SessionCreatedEventArgs> OnSessionCreated;

        public event EventHandler<BeforeConversionToDocumentEventArgs> OnBeforeConversionToDocument;

        public event EventHandler<AfterConversionToDocumentEventArgs> OnAfterConversionToDocument;

        public event EventHandler<BeforeConversionToEntityEventArgs> OnBeforeConversionToEntity;

        public event EventHandler<AfterConversionToEntityEventArgs> OnAfterConversionToEntity;

        private event EventHandler<FailedRequestEventArgs> _onFailedRequest;

        public event EventHandler<FailedRequestEventArgs> OnFailedRequest
        {
            add
            {
                AssertNotInitialized(nameof(OnFailedRequest));
                _onFailedRequest += value;
            }
            remove
            {
                AssertNotInitialized(nameof(OnFailedRequest));
                _onFailedRequest -= value;
            }
        }
        private event EventHandler<BeforeRequestEventArgs> _onBeforeRequest;
        public event EventHandler<BeforeRequestEventArgs> OnBeforeRequest
        {
            add
            {
                AssertNotInitialized(nameof(OnBeforeRequest));
                _onBeforeRequest += value;
            }
            remove
            {
                AssertNotInitialized(nameof(OnBeforeRequest));
                _onBeforeRequest -= value;
            }
        }
        
        private event EventHandler<SucceedRequestEventArgs> _onSucceedRequest;
        public event EventHandler<SucceedRequestEventArgs> OnSucceedRequest
        {
            add
            {
                AssertNotInitialized(nameof(OnSucceedRequest));
                _onSucceedRequest += value;
            }
            remove
            {
                AssertNotInitialized(nameof(OnSucceedRequest));
                _onSucceedRequest -= value;
            }
        }
        

        private event EventHandler<TopologyUpdatedEventArgs> _onTopologyUpdated;

        public event EventHandler<TopologyUpdatedEventArgs> OnTopologyUpdated
        {
            add
            {
                AssertNotInitialized(nameof(OnTopologyUpdated));
                _onTopologyUpdated += value;
            }
            remove
            {
                AssertNotInitialized(nameof(OnTopologyUpdated));
                _onTopologyUpdated -= value;
            }
        }

        /// <summary>
        /// The default database name
        /// </summary>
        public string Database
        {
            get => _database;
            set
            {
                AssertNotInitialized(nameof(Database));

                _database = value;
            }
        }

        /// <summary>
        /// The client certificate to use for authentication
        /// </summary>
        public X509Certificate2 Certificate
        {
            get => _certificate;
            set
            {
                AssertNotInitialized(nameof(Certificate));

                _certificate = value;
            }
        }

        public abstract RequestExecutor GetRequestExecutor(string databaseName = null);

        public abstract DatabaseSmuggler Smuggler { get; }

        public abstract IDisposable SetRequestTimeout(TimeSpan timeout, string database = null);

        /// <summary>
        /// Setup the context for aggressive caching.
        /// </summary>
        public IDisposable AggressivelyCache(string database = null)
        {
            return AggressivelyCacheFor(Conventions.AggressiveCache.Duration, Conventions.AggressiveCache.Mode, database);
        }

        protected void RegisterEvents(InMemoryDocumentSessionOperations session)
        {
            session.OnBeforeStore += OnBeforeStore;
            session.OnAfterSaveChanges += OnAfterSaveChanges;
            session.OnBeforeDelete += OnBeforeDelete;
            session.OnBeforeQuery += OnBeforeQuery;

            session.OnBeforeConversionToDocument += OnBeforeConversionToDocument;
            session.OnAfterConversionToDocument += OnAfterConversionToDocument;
            session.OnBeforeConversionToEntity += OnBeforeConversionToEntity;
            session.OnAfterConversionToEntity += OnAfterConversionToEntity;
        }

        protected internal void RegisterEvents(RequestExecutor requestExecutor)
        {
            requestExecutor.OnFailedRequest += _onFailedRequest;
            requestExecutor.OnBeforeRequest += _onBeforeRequest;
            requestExecutor.OnSucceedRequest += _onSucceedRequest;
            requestExecutor.OnTopologyUpdated += _onTopologyUpdated;
        }

        protected void AfterSessionCreated(InMemoryDocumentSessionOperations session)
        {
            OnSessionCreated?.Invoke(this, new SessionCreatedEventArgs(session));
        }

        public abstract MaintenanceOperationExecutor Maintenance { get; }
        public abstract OperationExecutor Operations { get; }
    }
}
