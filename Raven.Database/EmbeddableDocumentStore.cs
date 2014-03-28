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
using Raven.Server;

namespace Raven.Database
{
    public class EmbeddableDocumentStore2 : IDocumentStore
    {
        private readonly RavenDbServer server;

        public EmbeddableDocumentStore2()
        {
            server = new RavenDbServer();
        }

        public void Dispose()
        {
            server.Dispose();
        }

        public event EventHandler AfterDispose
        {
            add { server.DocumentStore.AfterDispose += value; }
            remove { server.DocumentStore.AfterDispose -= value; }
        }

        public bool WasDisposed
        {
            get
            {
                return server.DocumentStore.WasDisposed;
            }
        }

        public IDatabaseChanges Changes(string database = null)
        {
            return server.DocumentStore.Changes(database);
        }

        public IDisposable AggressivelyCacheFor(TimeSpan cacheDuration)
        {
            return server.DocumentStore.AggressivelyCacheFor(cacheDuration);
        }

        public IDisposable AggressivelyCache()
        {
            return server.DocumentStore.AggressivelyCache();
        }

        public IDisposable DisableAggressiveCaching()
        {
            return server.DocumentStore.DisableAggressiveCaching();
        }

        public IDisposable SetRequestsTimeoutFor(TimeSpan timeout)
        {
            return server.DocumentStore.SetRequestsTimeoutFor(timeout);
        }

        public NameValueCollection SharedOperationsHeaders
        {
            get { return server.DocumentStore.SharedOperationsHeaders; }
        }

        public HttpJsonRequestFactory JsonRequestFactory
        {
            get { return server.DocumentStore.JsonRequestFactory; }
        }

        public string Identifier
        {
            get { return server.DocumentStore.Identifier; }
            set { server.DocumentStore.Identifier = value; }
        }

        public IDocumentStore Initialize()
        {
            server.Initialize();
            return this;
        }

        public IAsyncDatabaseCommands AsyncDatabaseCommands
        {
            get { return server.DocumentStore.AsyncDatabaseCommands; }
        }

        public IAsyncDocumentSession OpenAsyncSession()
        {
            return server.DocumentStore.OpenAsyncSession();
        }

        public IAsyncDocumentSession OpenAsyncSession(string database)
        {
            return server.DocumentStore.OpenAsyncSession(database);
        }

        public IDocumentSession OpenSession()
        {
            return server.DocumentStore.OpenSession();
        }

        public IDocumentSession OpenSession(string database)
        {
            return server.DocumentStore.OpenSession(database);
        }

        public IDocumentSession OpenSession(OpenSessionOptions sessionOptions)
        {
            return server.DocumentStore.OpenSession(sessionOptions);
        }

        public IDatabaseCommands DatabaseCommands
        {
            get { return server.DocumentStore.DatabaseCommands; }
        }

        public void ExecuteIndex(AbstractIndexCreationTask indexCreationTask)
        {
            server.DocumentStore.ExecuteIndex(indexCreationTask);
        }

        public Task ExecuteIndexAsync(AbstractIndexCreationTask indexCreationTask)
        {
            return server.DocumentStore.ExecuteIndexAsync(indexCreationTask);
        }

        public void ExecuteTransformer(AbstractTransformerCreationTask transformerCreationTask)
        {
            server.DocumentStore.ExecuteTransformer(transformerCreationTask);
        }

        public Task ExecuteTransformerAsync(AbstractTransformerCreationTask transformerCreationTask)
        {
            return server.DocumentStore.ExecuteTransformerAsync(transformerCreationTask);
        }

        public DocumentConvention Conventions
        {
            get { return server.DocumentStore.Conventions; }
        }

        public string Url
        {
            get { return server.DocumentStore.Url; }
        }

        public Etag GetLastWrittenEtag()
        {
            return server.DocumentStore.GetLastWrittenEtag();
        }

        public BulkInsertOperation BulkInsert(string database = null, BulkInsertOptions options = null)
        {
            return server.DocumentStore.BulkInsert(database, options);
        }
    }
}
