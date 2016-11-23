using Raven.NewClient.Abstractions.Data;
using Raven.NewClient.Client.Changes;
using Raven.NewClient.Client.Connection;
using Raven.NewClient.Client.Connection.Async;
using Raven.NewClient.Client.Connection.Profiling;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Client.Listeners;
using Raven.NewClient.Client.Util;
using System;
using System.Collections.Generic;
using System.Collections.ObjectModel;
using System.Collections.Specialized;
using System.Linq;
using System.Threading.Tasks;
using Raven.NewClient.Abstractions.Util.Encryptors;
using Raven.NewClient.Client.Document;
using Raven.NewClient.Indexes;

namespace Raven.NewClient.Client
{

    /// <summary>
    /// Contains implementation of some IDocumentStore operations shared by DocumentStore implementations
    /// </summary>
    public abstract class DocumentStoreBase : IDocumentStore
    {
        protected DocumentStoreBase()
        {
            InitializeEncryptor();

            LastEtagHolder = new GlobalLastEtagHolder();
            AsyncSubscriptions = new AsyncDocumentSubscriptions(this);
            Subscriptions = new DocumentSubscriptions(this);
        }

        public DocumentSessionListeners Listeners
        {
            get { return listeners; }
        }

        public void SetListeners(DocumentSessionListeners newListeners)
        {
            listeners = newListeners;
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

        public abstract bool HasJsonRequestFactory { get; }
        public abstract HttpJsonRequestFactory JsonRequestFactory { get; }
        public abstract string Identifier { get; set; }
        public abstract IDocumentStore Initialize();
        public abstract IAsyncDatabaseCommands AsyncDatabaseCommands { get; }
        public abstract IAsyncDocumentSession OpenAsyncSession();
        public abstract IAsyncDocumentSession OpenAsyncSession(string database);
        public abstract IAsyncDocumentSession OpenAsyncSession(OpenSessionOptions sessionOptions);

        public abstract IDocumentSession OpenSession();
        public abstract IDocumentSession OpenSession(string database);
        public abstract IDocumentSession OpenSession(OpenSessionOptions sessionOptions);
        public abstract IDatabaseCommands DatabaseCommands { get; }

        /// <summary>
        /// Executes index creation.
        /// </summary>
        public virtual void ExecuteIndex(AbstractIndexCreationTask indexCreationTask)
        {
            indexCreationTask.Execute(DatabaseCommands, Conventions);
        }

        /// <summary>
        /// Executes index creation.
        /// </summary>
        public virtual Task ExecuteIndexAsync(AbstractIndexCreationTask indexCreationTask)
        {
            return indexCreationTask.ExecuteAsync(AsyncDatabaseCommands, Conventions);
        }

        /// <summary>
        /// Executes index creation in side-by-side mode.
        /// </summary>
        public virtual void SideBySideExecuteIndex(AbstractIndexCreationTask indexCreationTask, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            indexCreationTask.SideBySideExecute(DatabaseCommands, Conventions, minimumEtagBeforeReplace, replaceTimeUtc);
        }

        /// <summary>
        /// Executes index creation in side-by-side mode.
        /// </summary>
        public virtual Task SideBySideExecuteIndexAsync(AbstractIndexCreationTask indexCreationTask, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            return indexCreationTask.SideBySideExecuteAsync(AsyncDatabaseCommands, Conventions, minimumEtagBeforeReplace, replaceTimeUtc);
        }

        /// <summary>
        /// Executes transformer creation
        /// </summary>
        public virtual void ExecuteTransformer(AbstractTransformerCreationTask transformerCreationTask)
        {
            transformerCreationTask.Execute(DatabaseCommands, Conventions);
        }

        /// <summary>
        /// Executes transformer creation
        /// </summary>
        public virtual Task ExecuteTransformerAsync(AbstractTransformerCreationTask transformerCreationTask)
        {
            return transformerCreationTask.ExecuteAsync(AsyncDatabaseCommands, Conventions);
        }

        /// <summary>
        /// Executes indexes creation.
        /// </summary>
        public virtual void ExecuteIndexes(IList<AbstractIndexCreationTask> indexCreationTasks)
        {
            var indexesToAdd = IndexCreation.CreateIndexesToAdd(indexCreationTasks, Conventions);
            DatabaseCommands.PutIndexes(indexesToAdd);

            foreach (var task in indexCreationTasks)
                task.AfterExecute(DatabaseCommands, Conventions);
        }

        /// <summary>
        /// Executes indexes creation.
        /// </summary>
        public virtual async Task ExecuteIndexesAsync(List<AbstractIndexCreationTask> indexCreationTasks)
        {
            var indexesToAdd = IndexCreation.CreateIndexesToAdd(indexCreationTasks, Conventions);
            await AsyncDatabaseCommands.PutIndexesAsync(indexesToAdd).ConfigureAwait(false);

            foreach (var task in indexCreationTasks)
                await task.AfterExecuteAsync(AsyncDatabaseCommands, Conventions).ConfigureAwait(false);
        }

        /// <summary>
        /// Executes indexes creation in side-by-side mode.
        /// </summary>
        public virtual void SideBySideExecuteIndexes(IList<AbstractIndexCreationTask> indexCreationTasks, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var indexesToAdd = IndexCreation.CreateIndexesToAdd(indexCreationTasks, Conventions);
            DatabaseCommands.PutSideBySideIndexes(indexesToAdd, minimumEtagBeforeReplace, replaceTimeUtc);

            foreach (var task in indexCreationTasks)
                task.AfterExecute(DatabaseCommands, Conventions);
        }

        /// <summary>
        /// Executes indexes creation in side-by-side mode.
        /// </summary>
        public virtual async Task SideBySideExecuteIndexesAsync(List<AbstractIndexCreationTask> indexCreationTasks, long? minimumEtagBeforeReplace = null, DateTime? replaceTimeUtc = null)
        {
            var indexesToAdd = IndexCreation.CreateIndexesToAdd(indexCreationTasks, Conventions);
            await AsyncDatabaseCommands.PutSideBySideIndexesAsync(indexesToAdd, minimumEtagBeforeReplace, replaceTimeUtc).ConfigureAwait(false);

            foreach (var task in indexCreationTasks)
                task.AfterExecute(DatabaseCommands, Conventions);
        }

        private DocumentConvention conventions;

        /// <summary>
        /// Gets the conventions.
        /// </summary>
        /// <value>The conventions.</value>
        public virtual DocumentConvention Conventions
        {
            get { return conventions ?? (conventions = new DocumentConvention()); }
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


        ///<summary>
        /// Gets the etag of the last document written by any session belonging to this 
        /// document store
        ///</summary>
        public virtual long? GetLastWrittenEtag()
        {
            return LastEtagHolder.GetLastWrittenEtag();
        }

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

        private DocumentSessionListeners listeners = new DocumentSessionListeners();

        /// <summary>
        /// Registers the extended conversion listener.
        /// </summary>
        public DocumentStoreBase RegisterListener(IDocumentConversionListener conversionListener)
        {
            listeners.ConversionListeners = listeners.ConversionListeners.Concat(new[] { conversionListener, }).ToArray();
            return this;
        }

        /// <summary>
        /// Registers the query listener.
        /// </summary>
        /// <param name="queryListener">The query listener.</param>
        public DocumentStoreBase RegisterListener(IDocumentQueryListener queryListener)
        {
            listeners.QueryListeners = listeners.QueryListeners.Concat(new[] { queryListener }).ToArray();
            return this;
        }

        /// <summary>
        /// Registers the store listener.
        /// </summary>
        /// <param name="documentStoreListener">The document store listener.</param>
        public IDocumentStore RegisterListener(IDocumentStoreListener documentStoreListener)
        {
            listeners.StoreListeners = listeners.StoreListeners.Concat(new[] { documentStoreListener }).ToArray();
            return this;
        }

        /// <summary>
        /// Registers the delete listener.
        /// </summary>
        /// <param name="deleteListener">The delete listener.</param>
        public DocumentStoreBase RegisterListener(IDocumentDeleteListener deleteListener)
        {
            listeners.DeleteListeners = listeners.DeleteListeners.Concat(new[] { deleteListener }).ToArray();
            return this;
        }

        /// <summary>
        /// Registers the conflict listener.
        /// </summary>
        /// <param name="conflictListener">The conflict listener.</param>
        public DocumentStoreBase RegisterListener(IDocumentConflictListener conflictListener)
        {
            listeners.ConflictListeners = listeners.ConflictListeners.Concat(new[] { conflictListener }).ToArray();
            return this;
        }

        /// <summary>
        /// Gets a read-only collection of the registered query listeners.
        /// </summary>
        public ReadOnlyCollection<IDocumentQueryListener> RegisteredQueryListeners
        {
            get { return new ReadOnlyCollection<IDocumentQueryListener>(listeners.QueryListeners); }
        }

        /// <summary>
        /// Gets a read-only collection of the registered store listeners.
        /// </summary>
        public ReadOnlyCollection<IDocumentStoreListener> RegisteredStoreListeners
        {
            get { return new ReadOnlyCollection<IDocumentStoreListener>(listeners.StoreListeners); }
        }

        /// <summary>
        /// Gets a read-only collection of the registered delete listeners.
        /// </summary>
        public ReadOnlyCollection<IDocumentDeleteListener> RegisteredDeleteListeners
        {
            get { return new ReadOnlyCollection<IDocumentDeleteListener>(listeners.DeleteListeners); }
        }

        /// <summary>
        /// Gets a read-only collection of the registered conflict listeners.
        /// </summary>
        public ReadOnlyCollection<IDocumentConflictListener> RegisteredConflictListeners
        {
            get { return new ReadOnlyCollection<IDocumentConflictListener>(listeners.ConflictListeners); }
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

        protected readonly ProfilingContext profilingContext = new ProfilingContext();

        public ILastEtagHolder LastEtagHolder { get; set; }

        /// <summary>
        ///  Get the profiling information for the given id
        /// </summary>
        public ProfilingInformation GetProfilingInformationFor(Guid id)
        {
            return profilingContext.TryGet(id);
        }

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

        public abstract void InitializeProfiling();

        protected void RegisterEvents(InMemoryDocumentSessionOperations session)
        {
            session.OnBeforeStore += OnBeforeStore;
            session.OnAfterStore += OnAfterStore;
            session.OnBeforeDelete += OnBeforeDelete;
            session.OnBeforeQueryExecuted += OnBeforeQueryExecuted;
        }


    }
}
