using System;
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
using Raven.Client.Documents.Subscriptions;
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

        /// <summary>
        /// 
        /// </summary>
        public abstract event EventHandler AfterDispose;

        /// <summary>
        /// Whether the instance has been disposed
        /// </summary>
        public bool WasDisposed { get; protected set; }

        /// <summary>
        /// Subscribe to change notifications from the server
        /// </summary>

        public abstract IDisposable AggressivelyCacheFor(TimeSpan cacheDuration, string database = null);

        public abstract IDatabaseChanges Changes(string database = null);

        public abstract IDisposable DisableAggressiveCaching(string database = null);

        public abstract string Identifier { get; set; }
        public abstract IDocumentStore Initialize();
        public abstract IAsyncDocumentSession OpenAsyncSession();
        public abstract IAsyncDocumentSession OpenAsyncSession(string database);
        public abstract IAsyncDocumentSession OpenAsyncSession(SessionOptions sessionOptions);

        public abstract IDocumentSession OpenSession();
        public abstract IDocumentSession OpenSession(string database);
        public abstract IDocumentSession OpenSession(SessionOptions sessionOptions);

        /// <summary>
        /// Executes index creation.
        /// </summary>
        public virtual void ExecuteIndex(AbstractIndexCreationTask task)
        {
            AsyncHelpers.RunSync(() => ExecuteIndexAsync(task));
        }

        /// <summary>
        /// Executes index creation.
        /// </summary>
        public virtual Task ExecuteIndexAsync(AbstractIndexCreationTask task, CancellationToken token = default(CancellationToken))
        {
            return task.ExecuteAsync(this, Conventions, token);
        }

        /// <summary>
        /// Executes indexes creation.
        /// </summary>
        public virtual void ExecuteIndexes(IEnumerable<AbstractIndexCreationTask> tasks)
        {
            AsyncHelpers.RunSync(() => ExecuteIndexesAsync(tasks));
        }

        /// <summary>
        /// Executes indexes creation.
        /// </summary>
        public virtual Task ExecuteIndexesAsync(IEnumerable<AbstractIndexCreationTask> tasks, CancellationToken token = default(CancellationToken))
        {
            var indexesToAdd = IndexCreation.CreateIndexesToAdd(tasks, Conventions);

            return Admin.SendAsync(new PutIndexesOperation(indexesToAdd), token);
        }

        private DocumentConventions _conventions;

        /// <summary>
        /// Gets the conventions.
        /// </summary>
        /// <value>The conventions.</value>
        public virtual DocumentConventions Conventions
        {
            get => _conventions ?? (_conventions = new DocumentConventions());
            set => _conventions = value;
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
                if (value == null) throw new ArgumentNullException(nameof(value));
                for (var i = 0; i < value.Length; i++)
                {
                    if(value[i] == null)
                        throw new ArgumentNullException(nameof(value), "Urls cannot contain null");

                    if(Uri.TryCreate(value[i], UriKind.Absolute, out var _) == false)
                        throw new ArgumentException(value[i] + " is no a valid url");
                    value[i] = value[i].TrimEnd('/');
                }
                _urls = value;
            }
        }

        protected bool Initialized;
        private X509Certificate2 _certificate;

        public abstract BulkInsertOperation BulkInsert(string database = null);

        public IReliableSubscriptions Subscriptions { get; }

        protected void EnsureNotClosed()
        {
            if (WasDisposed)
                throw new ObjectDisposedException(GetType().Name, "The document store has already been disposed and cannot be used");
        }

        protected void AssertInitialized()
        {
            if (Initialized == false)
                throw new InvalidOperationException("You cannot open a session or access the database commands before initializing the document store. Did you forget calling Initialize()?");
        }

        protected virtual void AfterSessionCreated(InMemoryDocumentSessionOperations session)
        {
            var onSessionCreatedInternal = SessionCreatedInternal;
            onSessionCreatedInternal?.Invoke(session);
        }

        ///<summary>
        /// Internal notification for integration tools, mainly
        ///</summary>
        public event Action<InMemoryDocumentSessionOperations> SessionCreatedInternal;
        public event Action<string> TopologyUpdatedInternal;
        public event EventHandler<BeforeStoreEventArgs> OnBeforeStore;
        public event EventHandler<AfterStoreEventArgs> OnAfterStore;
        public event EventHandler<BeforeDeleteEventArgs> OnBeforeDelete;
        public event EventHandler<BeforeQueryExecutedEventArgs> OnBeforeQueryExecuted;

        /// <summary>
        /// Gets or sets the default database name.
        /// </summary>
        /// <value>The default database name.</value>
        public string Database { get; set; }

        /// <summary>
        /// The client certificate to use for authentication
        /// </summary>
        public X509Certificate2 Certificate
        {
            get => _certificate;
            set
            {
                if(Initialized)
                    throw new InvalidOperationException("You cannot change the certificate after the document store was initialized");
                _certificate = value;
            }
        }

        public abstract RequestExecutor GetRequestExecutor(string databaseName = null);

        public abstract IDisposable SetRequestsTimeout(TimeSpan timeout, string database = null);

        /// <summary>
        /// Setup the context for aggressive caching.
        /// </summary>
        public IDisposable AggressivelyCache(string database = null)
        {
            return AggressivelyCacheFor(TimeSpan.FromDays(1), database);
        }

        protected void RegisterEvents(InMemoryDocumentSessionOperations session)
        {
            session.OnBeforeStore += OnBeforeStore;
            session.OnAfterStore += OnAfterStore;
            session.OnBeforeDelete += OnBeforeDelete;
            session.OnBeforeQueryExecuted += OnBeforeQueryExecuted;
        }

        public abstract AdminOperationExecutor Admin { get; }
        public abstract OperationExecutor Operations { get; }

        protected void OnTopologyUpdatedInternal(string databaseName)
        {
            TopologyUpdatedInternal?.Invoke(databaseName);
        }
    }
}
