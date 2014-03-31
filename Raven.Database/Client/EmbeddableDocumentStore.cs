// -----------------------------------------------------------------------
//  <copyright file="EmbeddableDocumentStore.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database;
using Raven.Database.Client;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Json.Linq;

namespace Raven.Client.Embedded
{
    public class EmbeddableDocumentStore : IDocumentStore
    {
        private IDocumentStore _inner;
        public RavenConfiguration Configuration { get; set; }

        private OwinHttpServer httpServer;

        readonly DocumentSessionListeners listeners = new DocumentSessionListeners();

        public EmbeddableDocumentStore()
        {
            Conventions = new DocumentConvention();
        }

        private IDocumentStore Inner
        {
            get
            {
                if(_inner == null)
                    throw new InvalidOperationException("You cannot open a session or access the database commands before initializing the document store. Did you forget calling Initialize()?");
                return _inner;
            }
        }

        public IDocumentStore Initialize()
        {
            if (string.IsNullOrEmpty(Url) == false)
            {
                _inner = new DocumentStore
                {
                    Url = Url,
                    Conventions = Conventions
                }.Initialize();
            }
            else
            {
                Configuration = new RavenConfiguration();
                _inner = new EmbeddedDocumentStore
                {
                    DataDirectory = DataDirectory,
                    Conventions = Conventions,
                    Configuration = Configuration
                };

                if (UseEmbeddedHttpServer)
                {
                    SetStudioConfigToAllowSingleDb();
                    httpServer = new OwinHttpServer(Configuration, DocumentDatabase);
                }
            }
            return this;
        }

        public DocumentDatabase DocumentDatabase
        {
            get
            {
                var eds = Inner as EmbeddedDocumentStore;
                if (eds != null)
                    return eds.DocumentDatabase;
                return null;
            }
        }

        public DocumentConvention Conventions
        {
            get; private set;
        }

        public string DataDirectory { get; set; }
        public string Url
        {
            get; set;
        }
        public void Dispose()
        {
            Inner.Dispose();
        }

        public event EventHandler AfterDispose
        {
            add { Inner.AfterDispose += value; }
            remove { Inner.AfterDispose -= value; }
        }
        public bool WasDisposed
        {
            get { return Inner.WasDisposed; }
        }
        public IDatabaseChanges Changes(string database = null)
        {
            return Inner.Changes(database);
        }

        public IDisposable AggressivelyCacheFor(TimeSpan cacheDuration)
        {
            return Inner.AggressivelyCacheFor(cacheDuration);
        }

        public IDisposable AggressivelyCache()
        {
            return Inner.AggressivelyCache();
        }

        public IDisposable DisableAggressiveCaching()
        {
            return Inner.DisableAggressiveCaching();
        }

        public IDisposable SetRequestsTimeoutFor(TimeSpan timeout)
        {
            return Inner.SetRequestsTimeoutFor(timeout);
        }

        public NameValueCollection SharedOperationsHeaders
        {
            get { return Inner.SharedOperationsHeaders; }
        }
        public HttpJsonRequestFactory JsonRequestFactory
        {
            get { return Inner.JsonRequestFactory; }
        }
        public string Identifier
        {
            get { return Inner.Identifier; }
            set { Inner.Identifier = value; }
        }

        public IAsyncDatabaseCommands AsyncDatabaseCommands
        {
            get { return Inner.AsyncDatabaseCommands; }
        }
        public IAsyncDocumentSession OpenAsyncSession()
        {
            return Inner.OpenAsyncSession();
        }

        public IAsyncDocumentSession OpenAsyncSession(string database)
        {
            return Inner.OpenAsyncSession(database);
        }

        public IDocumentSession OpenSession()
        {
            return Inner.OpenSession();
        }

        public IDocumentSession OpenSession(string database)
        {
            return Inner.OpenSession(database);
        }

        public IDocumentSession OpenSession(OpenSessionOptions sessionOptions)
        {
            return Inner.OpenSession(sessionOptions);
        }

        public IDatabaseCommands DatabaseCommands
        {
            get { return Inner.DatabaseCommands; }
        }
        public void ExecuteIndex(AbstractIndexCreationTask indexCreationTask)
        {
            Inner.ExecuteIndex(indexCreationTask);
        }

        public Task ExecuteIndexAsync(AbstractIndexCreationTask indexCreationTask)
        {
            return Inner.ExecuteIndexAsync(indexCreationTask);
        }

        public void ExecuteTransformer(AbstractTransformerCreationTask transformerCreationTask)
        {
            Inner.ExecuteTransformer(transformerCreationTask);
        }

        public Task ExecuteTransformerAsync(AbstractTransformerCreationTask transformerCreationTask)
        {
            return Inner.ExecuteTransformerAsync(transformerCreationTask);
        }

        public Etag GetLastWrittenEtag()
        {
            return Inner.GetLastWrittenEtag();
        }

        public BulkInsertOperation BulkInsert(string database = null, BulkInsertOptions options = null)
        {
            return Inner.BulkInsert(database, options);
        }

        /// <summary>
        ///     Let the studio knows that it shouldn't display the warning about sys db access
        /// </summary>
        public void SetStudioConfigToAllowSingleDb()
        {
            if (DocumentDatabase == null)
                return;
            JsonDocument jsonDocument = DocumentDatabase.Documents.Get("Raven/StudioConfig", null);
            RavenJObject doc;
            RavenJObject metadata;
            if (jsonDocument == null)
            {
                doc = new RavenJObject();
                metadata = new RavenJObject();
            }
            else
            {
                doc = jsonDocument.DataAsJson;
                metadata = jsonDocument.Metadata;
            }

            doc["WarnWhenUsingSystemDatabase"] = false;

            DocumentDatabase.Documents.Put("Raven/StudioConfig", null, doc, metadata, null);
        }


        /// <summary>
        /// Expose the internal http server, if used
        /// </summary>
        public OwinHttpServer HttpServer
        {
            get { return httpServer; }
        }
        
        ///<summary>
        /// Whatever we should also host an HTTP endpoint for the document database
        ///</summary>
        public bool UseEmbeddedHttpServer { get; set; }



        public IDocumentStore RegisterListener(Listeners.IDocumentConversionListener conversionListener)
        {
            listeners.ConversionListeners = listeners.ConversionListeners.Concat(new[] { conversionListener, }).ToArray();
            return this;
        }
    }
}