using System;
using System.Collections.Generic;
using System.Collections.Specialized;
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
using Raven.Client.Util.Encryption;
using Sparrow.Json;

namespace Raven.Client.Documents
{

    /// <summary>
    /// Contains implementation of some IDocumentStore operations shared by DocumentStore implementations
    /// </summary>
    public abstract class DocumentStoreBase : IDocumentStore
    {
        protected DocumentStoreBase()
        {
            InitializeEncryptor();

            AsyncSubscriptions = new AsyncDocumentSubscriptions(this);
            Subscriptions = new DocumentSubscriptions(this);
        }

        public abstract void Dispose();

        /// <summary>
        /// 
        /// </summary>
        public abstract event EventHandler AfterDispose;

        /// <summary>
        /// Whatever the instance has been disposed
        /// </summary>
        public bool WasDisposed { get; protected set; }

        /// <summary>
        /// Subscribe to change notifications from the server
        /// </summary>

        public abstract IDisposable AggressivelyCacheFor(TimeSpan cacheDuration);

        public abstract IDatabaseChanges Changes(string database = null);

        public abstract IDisposable DisableAggressiveCaching();

        public abstract IDisposable SetRequestsTimeoutFor(TimeSpan timeout);

        /// <summary>
        /// Gets the shared operations headers.
        /// </summary>
        /// <value>The shared operations headers.</value>
        public virtual NameValueCollection SharedOperationsHeaders { get; protected set; }

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
        public virtual void ExecuteIndex(AbstractIndexCreationTask indexCreationTask)
        {
            indexCreationTask.Execute(this, Conventions);
        }

        /// <summary>
        /// Executes index creation.
        /// </summary>
        public virtual Task ExecuteIndexAsync(AbstractIndexCreationTask indexCreationTask)
        {
            return indexCreationTask.ExecuteAsync(this, Conventions);
        }

        /// <summary>
        /// Executes index creation in side-by-side mode.
        /// </summary>
        public virtual void SideBySideExecuteIndex(AbstractIndexCreationTask indexCreationTask, long? minimumEtagBeforeReplace = null)
        {
            indexCreationTask.SideBySideExecute(this, Conventions, minimumEtagBeforeReplace);
        }

        /// <summary>
        /// Executes index creation in side-by-side mode.
        /// </summary>
        public virtual Task SideBySideExecuteIndexAsync(AbstractIndexCreationTask indexCreationTask, long? minimumEtagBeforeReplace = null)
        {
            return indexCreationTask.SideBySideExecuteAsync(this, Conventions, minimumEtagBeforeReplace);
        }

        /// <summary>
        /// Executes transformer creation
        /// </summary>
        public virtual void ExecuteTransformer(AbstractTransformerCreationTask transformerCreationTask)
        {
            transformerCreationTask.Execute(this, Conventions);
        }

        /// <summary>
        /// Executes transformer creation
        /// </summary>
        public virtual Task ExecuteTransformerAsync(AbstractTransformerCreationTask transformerCreationTask)
        {
            return transformerCreationTask.ExecuteAsync(this, Conventions);
        }

        /// <summary>
        /// Executes indexes creation.
        /// </summary>
        public virtual void ExecuteIndexes(IList<AbstractIndexCreationTask> indexCreationTasks)
        {
            var indexesToAdd = IndexCreation.CreateIndexesToAdd(indexCreationTasks, Conventions);
            var requestExecuter = GetRequestExecuter();

            JsonOperationContext context;

            using (requestExecuter.ContextPool.AllocateOperationContext(out context))
            {
                var admin = new AdminOperationExecuter(this, requestExecuter, context);
                var putIndexesOperation = new PutIndexesOperation(indexesToAdd);
                admin.Send(putIndexesOperation);
            }
        }

        /// <summary>
        /// Executes indexes creation.
        /// </summary>
        public virtual async Task ExecuteIndexesAsync(List<AbstractIndexCreationTask> indexCreationTasks)
        {
            var indexesToAdd = IndexCreation.CreateIndexesToAdd(indexCreationTasks, Conventions);
            var requestExecuter = GetRequestExecuter();

            JsonOperationContext context;

            using (requestExecuter.ContextPool.AllocateOperationContext(out context))
            {
                var admin = new AdminOperationExecuter(this, requestExecuter, context);
                var putIndexesOperation = new PutIndexesOperation(indexesToAdd);
                await admin.SendAsync(putIndexesOperation).ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Executes indexes creation in side-by-side mode.
        /// </summary>
        public virtual void SideBySideExecuteIndexes(IList<AbstractIndexCreationTask> indexCreationTasks, long? minimumEtagBeforeReplace = null)
        {
            var indexesToAdd = IndexCreation.CreateIndexesToAdd(indexCreationTasks, Conventions, minimumEtagBeforeReplace);
            var requestExecuter = GetRequestExecuter();

            JsonOperationContext context;

            using (requestExecuter.ContextPool.AllocateOperationContext(out context))
            {
                var admin = new AdminOperationExecuter(this, requestExecuter, context);
                var putIndexesOperation = new PutIndexesOperation(indexesToAdd);
                admin.Send(putIndexesOperation);
            }
        }

        /// <summary>
        /// Executes indexes creation in side-by-side mode.
        /// </summary>
        public virtual async Task SideBySideExecuteIndexesAsync(List<AbstractIndexCreationTask> indexCreationTasks, long? minimumEtagBeforeReplace = null)
        {
            var indexesToAdd = IndexCreation.CreateIndexesToAdd(indexCreationTasks, Conventions, minimumEtagBeforeReplace);
            var requestExecuter = GetRequestExecuter();

            JsonOperationContext context;

            using (requestExecuter.ContextPool.AllocateOperationContext(out context))
            {
                var admin = new AdminOperationExecuter(this, requestExecuter, context);
                var putIndexesOperation = new PutIndexesOperation(indexesToAdd);
                await admin.SendAsync(putIndexesOperation).ConfigureAwait(false);
            }
        }

        private DocumentConventions conventions;

        /// <summary>
        /// Gets the conventions.
        /// </summary>
        /// <value>The conventions.</value>
        public virtual DocumentConventions Conventions
        {
            get { return conventions ?? (conventions = new DocumentConventions()); }
            set { conventions = value; }
        }

        private string url;

        /// <summary>
        /// Gets or sets the URL.
        /// </summary>
        public virtual string Url
        {
            get { return url; }
            set { url = value.EndsWith("/") ? value.Substring(0, value.Length - 1) : value; }
        }

        /// <summary>
        /// Failover servers used by replication informers when cannot fetch the list of replication 
        /// destinations if a master server is down.
        /// </summary>
        public FailoverServers FailoverServers { get; set; }

        /// <summary>
        /// Whenever or not we will use FIPS compliant encryption algorithms (must match server settings).
        /// </summary>
        public bool UseFipsEncryptionAlgorithms { get; set; }

        protected bool initialized;

        public abstract BulkInsertOperation BulkInsert(string database = null);

        public IAsyncReliableSubscriptions AsyncSubscriptions { get; private set; }
        public IReliableSubscriptions Subscriptions { get; private set; }

        protected void EnsureNotClosed()
        {
            if (WasDisposed)
                throw new ObjectDisposedException(GetType().Name, "The document store has already been disposed and cannot be used");
        }

        protected void AssertInitialized()
        {
            if (!initialized)
                throw new InvalidOperationException("You cannot open a session or access the database commands before initializing the document store. Did you forget calling Initialize()?");
        }

        protected virtual void AfterSessionCreated(InMemoryDocumentSessionOperations session)
        {
            var onSessionCreatedInternal = SessionCreatedInternal;
            if (onSessionCreatedInternal != null)
                onSessionCreatedInternal(session);
        }

        ///<summary>
        /// Internal notification for integration tools, mainly
        ///</summary>
        public event Action<InMemoryDocumentSessionOperations> SessionCreatedInternal;
        public event EventHandler<BeforeStoreEventArgs> OnBeforeStore;
        public event EventHandler<AfterStoreEventArgs> OnAfterStore;
        public event EventHandler<BeforeDeleteEventArgs> OnBeforeDelete;
        public event EventHandler<BeforeQueryExecutedEventArgs> OnBeforeQueryExecuted;

        public abstract string DefaultDatabase { get; set; }

        public abstract RequestExecuter GetRequestExecuter(string databaseName = null);

        /// <summary>
        /// Setup the context for aggressive caching.
        /// </summary>
        public IDisposable AggressivelyCache()
        {
            return AggressivelyCacheFor(TimeSpan.FromDays(1));
        }

        protected void InitializeEncryptor()
        {
            var setting = ConfigurationManager.GetAppSetting("Raven/Encryption/FIPS");

            bool fips;
            if (string.IsNullOrEmpty(setting) || !bool.TryParse(setting, out fips))
                fips = UseFipsEncryptionAlgorithms;

            Encryptor.Initialize(fips);
        }

        protected void RegisterEvents(InMemoryDocumentSessionOperations session)
        {
            session.OnBeforeStore += OnBeforeStore;
            session.OnAfterStore += OnAfterStore;
            session.OnBeforeDelete += OnBeforeDelete;
            session.OnBeforeQueryExecuted += OnBeforeQueryExecuted;
        }

        public abstract AdminOperationExecuter Admin { get; }
    }
}
