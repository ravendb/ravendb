using System;
using System.Collections.Specialized;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Client.Listeners;
using Raven.Database.Config;
using Raven.Json.Linq;
using Raven.Server;

namespace Raven.Database.Client
{
    public class EmbeddedDocumentStore : IDocumentStore
    {
        private readonly RavenDbServer server;

        public EmbeddedDocumentStore()
        {
            server = new RavenDbServer();
        }

        /// <summary>
        ///     Whatever we should also host an HTTP endpoint for the document database
        /// </summary>
        public bool UseEmbeddedHttpServer
        {
            get { return server.UseEmbeddedHttpServer; }
            set { server.UseEmbeddedHttpServer = value; }
        }

        public InMemoryRavenConfiguration Configuration
        {
            get { return server.Configuration; }
            set { server.Configuration = value; }
        }

        public DocumentDatabase DocumentDatabase
        {
            get
            {
                //TODO oren: what should be returned here?
                return server.SystemDatabase;
            }
        }

        public string ConnectionStringName
        {
            get { return server.DocumentStore.ConnectionStringName; }
            set { server.DocumentStore.ConnectionStringName = value; }
        }

        /// <summary>
        ///     Run RavenDB in an embedded mode, using in memory only storage.
        ///     This is useful for unit tests, since it is very fast.
        /// </summary>
        public bool RunInMemory
        {
            get { return server.Configuration.RunInMemory; }
            set { server.Configuration.RunInMemory = value; }
        }

        /// <summary>
        ///     Run RavenDB in embedded mode, using the specified directory for data storage
        /// </summary>
        /// <value>The data directory.</value>
        public string DataDirectory
        {
            get { return Configuration.DataDirectory; }
            set { Configuration.DataDirectory = value; }
        }

        /// <summary>
        ///     Performs application-defined tasks associated with freeing, releasing, or resetting unmanaged resources.
        /// </summary>
        /// <filterpriority>2</filterpriority>
        public void Dispose()
        {
            server.Dispose();
        }

        /// <summary>
        /// Called after dispose is completed
        /// </summary>
        public event EventHandler AfterDispose
        {
            add { server.DocumentStore.AfterDispose += value; }
            remove { server.DocumentStore.AfterDispose -= value; }
        }

        /// <summary>
        /// Whatever the instance has been disposed
        /// </summary>
        public bool WasDisposed
        {
            get { return server.DocumentStore.WasDisposed; }
        }

        /// <summary>
        ///     Subscribe to change notifications from the server
        /// </summary>
        public IDatabaseChanges Changes(string database = null)
        {
            return server.DocumentStore.Changes(database);
        }

        /// <summary>
        ///     Setup the context for aggressive caching.
        /// </summary>
        /// <param name="cacheDuration">Specify the aggressive cache duration</param>
        /// <remarks>
        ///     Aggressive caching means that we will not check the server to see whatever the response
        ///     we provide is current or not, but will serve the information directly from the local cache
        ///     without touching the server.
        /// </remarks>
        public IDisposable AggressivelyCacheFor(TimeSpan cacheDuration)
        {
            return server.DocumentStore.AggressivelyCacheFor(cacheDuration);
        }

        /// <summary>
        ///     Setup the context for aggressive caching.
        /// </summary>
        /// <remarks>
        ///     Aggressive caching means that we will not check the server to see whatever the response
        ///     we provide is current or not, but will serve the information directly from the local cache
        ///     without touching the server.
        /// </remarks>
        public IDisposable AggressivelyCache()
        {
            return server.DocumentStore.AggressivelyCache();
        }

        /// <summary>
        ///     Setup the context for no aggressive caching
        /// </summary>
        /// <remarks>
        ///     This is mainly useful for internal use inside RavenDB, when we are executing
        ///     queries that has been marked with WaitForNonStaleResults, we temporarily disable
        ///     aggressive caching.
        /// </remarks>
        public IDisposable DisableAggressiveCaching()
        {
            return server.DocumentStore.DisableAggressiveCaching();
        }

        /// <summary>
        ///     Setup the WebRequest timeout for the session
        /// </summary>
        /// <param name="timeout">Specify the timeout duration</param>
        /// <remarks>
        ///     Sets the timeout for the JsonRequest.  Scoped to the Current Thread.
        /// </remarks>
        public IDisposable SetRequestsTimeoutFor(TimeSpan timeout)
        {
            return server.DocumentStore.SetRequestsTimeoutFor(timeout);
        }

        /// <summary>
        ///     Gets the shared operations headers.
        /// </summary>
        /// <value>The shared operations headers.</value>
        public NameValueCollection SharedOperationsHeaders
        {
            get { return server.DocumentStore.SharedOperationsHeaders; }
        }

        /// <summary>
        ///     Get the <see cref="HttpJsonRequestFactory" /> for this store
        /// </summary>
        public HttpJsonRequestFactory JsonRequestFactory
        {
            get { return server.DocumentStore.JsonRequestFactory; }
        }

        /// <summary>
        ///     Gets or sets the identifier for this store.
        /// </summary>
        /// <value>The identifier.</value>
        public string Identifier
        {
            get { return server.DocumentStore.Identifier ?? (RunInMemory ? "memory #" + GetHashCode() : DataDirectory); }
            set { server.DocumentStore.Identifier = value; }
        }

        /// <summary>
        ///     Initializes this instance.
        /// </summary>
        /// <returns></returns>
        public IDocumentStore Initialize()
        {
            server.Initialize();
            return this;
        }

        /// <summary>
        ///     Gets the async database commands.
        /// </summary>
        /// <value>The async database commands.</value>
        public IAsyncDatabaseCommands AsyncDatabaseCommands
        {
            get { return server.DocumentStore.AsyncDatabaseCommands; }
        }

        /// <summary>
        ///     Opens the async session.
        /// </summary>
        /// <returns></returns>
        public IAsyncDocumentSession OpenAsyncSession()
        {
            return server.DocumentStore.OpenAsyncSession();
        }

        /// <summary>
        ///     Opens the async session.
        /// </summary>
        /// <returns></returns>
        public IAsyncDocumentSession OpenAsyncSession(string database)
        {
            return server.DocumentStore.OpenAsyncSession(database);
        }

        /// <summary>
        ///     Opens the session.
        /// </summary>
        /// <returns></returns>
        public IDocumentSession OpenSession()
        {
            return server.DocumentStore.OpenSession();
        }

        /// <summary>
        ///     Opens the session for a particular database
        /// </summary>
        public IDocumentSession OpenSession(string database)
        {
            return server.DocumentStore.OpenSession(database);
        }

        /// <summary>
        ///     Opens the session with the specified options.
        /// </summary>
        public IDocumentSession OpenSession(OpenSessionOptions sessionOptions)
        {
            return server.DocumentStore.OpenSession(sessionOptions);
        }

        /// <summary>
        ///     Gets the database commands.
        /// </summary>
        /// <value>The database commands.</value>
        public IDatabaseCommands DatabaseCommands
        {
            get { return server.DocumentStore.DatabaseCommands; }
        }

        /// <summary>
        ///     Executes the index creation.
        /// </summary>
        public void ExecuteIndex(AbstractIndexCreationTask indexCreationTask)
        {
            server.DocumentStore.ExecuteIndex(indexCreationTask);
        }

        /// <summary>
        ///     Executes the index creation.
        /// </summary>
        /// <param name="indexCreationTask"></param>
        public Task ExecuteIndexAsync(AbstractIndexCreationTask indexCreationTask)
        {
            return server.DocumentStore.ExecuteIndexAsync(indexCreationTask);
        }

        /// <summary>
        ///     Executes the transformer creation
        /// </summary>
        public void ExecuteTransformer(AbstractTransformerCreationTask transformerCreationTask)
        {
            server.DocumentStore.ExecuteTransformer(transformerCreationTask);
        }

        public Task ExecuteTransformerAsync(AbstractTransformerCreationTask transformerCreationTask)
        {
            return server.DocumentStore.ExecuteTransformerAsync(transformerCreationTask);
        }

        /// <summary>
        ///     Gets the conventions.
        /// </summary>
        /// <value>The conventions.</value>
        public DocumentConvention Conventions
        {
            get { return server.DocumentStore.Conventions; }
            set { server.DocumentStore.Conventions = value; }
        }

        /// <summary>
        ///     Gets the URL.
        /// </summary>
        public string Url
        {
            get { return server.Url; }
            set { server.Url = value; }
        }

        /// <summary>
        ///     Gets the etag of the last document written by any session belonging to this
        ///     document store
        /// </summary>
        public Etag GetLastWrittenEtag()
        {
            return server.DocumentStore.GetLastWrittenEtag();
        }

        public BulkInsertOperation BulkInsert(string database = null, BulkInsertOptions options = null)
        {
            return server.DocumentStore.BulkInsert(database, options);
        }

        public DocumentSessionListeners Listeners { get { return server.DocumentStore.Listeners; } }
        public void SetListeners(DocumentSessionListeners listeners)
        {
            server.DocumentStore.SetListeners(listeners);
        }

        public string DefaultDatabase
        {
            get { return server.DocumentStore.DefaultDatabase; }
            set { server.DocumentStore.DefaultDatabase = value; }
        }
        public Guid ResourceManagerId
        {
            get { return server.DocumentStore.ResourceManagerId; }
            set { server.DocumentStore.ResourceManagerId = value; }
        }
        public bool EnlistInDistributedTransactions
        {
            get { return server.DocumentStore.EnlistInDistributedTransactions; }
            set { server.DocumentStore.EnlistInDistributedTransactions = value; }
        }

        /// <summary>
        ///     Registers the store listener.
        /// </summary>
        /// <param name="documentStoreListener">The document store listener.</param>
        public IDocumentStore RegisterListener(IDocumentStoreListener documentStoreListener)
        {
            return server.DocumentStore.RegisterListener(documentStoreListener);
        }

        /// <summary>
        ///     Registers the query listener.
        /// </summary>
        /// <param name="queryListener">The query listener.</param>
        public DocumentStoreBase RegisterListener(IDocumentQueryListener queryListener)
        {
            return server.DocumentStore.RegisterListener(queryListener);
        }

        /// <summary>
        ///     Registers the delete listener.
        /// </summary>
        /// <param name="deleteListener">The delete listener.</param>
        public DocumentStoreBase RegisterListener(IDocumentDeleteListener deleteListener)
        {
            return server.DocumentStore.RegisterListener(deleteListener);
        }

        /// <summary>
        ///     Registers the conversion listener.
        /// </summary>
        public DocumentStoreBase RegisterListener(IDocumentConversionListener documentConversionListener)
        {
            return server.DocumentStore.RegisterListener(documentConversionListener);
        }

        /// <summary>
        ///     Registers the conflict listener.
        /// </summary>
        /// <param name="conflictListener">The conflict listener.</param>
        public DocumentStoreBase RegisterListener(IDocumentConflictListener conflictListener)
        {
            return server.DocumentStore.RegisterListener(conflictListener);
        }
    }
}