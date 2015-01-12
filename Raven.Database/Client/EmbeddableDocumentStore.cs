// -----------------------------------------------------------------------
//  <copyright file="EmbeddableDocumentStore.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Collections.Specialized;
using System.Net;
using System.Threading.Tasks;
using Raven.Abstractions.Data;
using Raven.Client.Changes;
using Raven.Client.Connection;
using Raven.Client.Connection.Async;
using Raven.Client.Connection.Profiling;
using Raven.Client.Document;
using Raven.Client.FileSystem;
using Raven.Client.Indexes;
using Raven.Client.Listeners;
using Raven.Database;
using Raven.Database.Client;
using Raven.Database.Config;
using Raven.Server;

namespace Raven.Client.Embedded
{
    public class EmbeddableDocumentStore : IDocumentStore
    {
        private IDocumentStore _inner;
        private string _connectionStringName;
        public RavenConfiguration Configuration { get; set; }

        public string ConnectionStringName
        {
            get { return _connectionStringName; }
            set
            {
                _connectionStringName = value;
                HandleConnectionStringOptions();
            }
        }

        protected void HandleConnectionStringOptions()
        {
            var parser = ConnectionStringParser<EmbeddedRavenConnectionStringOptions>.FromConnectionStringName(ConnectionStringName);
            parser.Parse();
            var options = parser.ConnectionStringOptions;

            if(options.ResourceManagerId != Guid.Empty)
                ResourceManagerId = options.ResourceManagerId;
            if (options.Credentials != null)
                Credentials = options.Credentials;
            if (string.IsNullOrEmpty(options.Url) == false)
                Url = options.Url;
            if (string.IsNullOrEmpty(options.DefaultDatabase) == false)
                DefaultDatabase = options.DefaultDatabase;
            if (string.IsNullOrEmpty(options.ApiKey) == false)
                ApiKey = options.ApiKey;

            EnlistInDistributedTransactions = options.EnlistInDistributedTransactions;
            var embeddedRavenConnectionStringOptions = parser.ConnectionStringOptions as EmbeddedRavenConnectionStringOptions;

            if (embeddedRavenConnectionStringOptions == null)
                return;

            if (string.IsNullOrEmpty(embeddedRavenConnectionStringOptions.DataDirectory) == false)
                DataDirectory = embeddedRavenConnectionStringOptions.DataDirectory;

            RunInMemory = embeddedRavenConnectionStringOptions.RunInMemory;
        }

        public bool EnlistInDistributedTransactions { get; set; }

        public string ApiKey { get; set; }

        public string DefaultDatabase { get; set; }

        public ICredentials Credentials { get; set; }


        public EmbeddableDocumentStore()
        {
            Conventions = new DocumentConvention();
            Listeners = new DocumentSessionListeners();
            Configuration = new RavenConfiguration();
            EnlistInDistributedTransactions = true;
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
            if (_inner != null)
                return this;

			AssertValidConfiguration();

            if (string.IsNullOrEmpty(Url) == false)
            {
                _inner = new DocumentStore
                {
                    Url = Url,
                    Conventions = Conventions,
                    ResourceManagerId = ResourceManagerId,
                    DefaultDatabase = DefaultDatabase,
                    Credentials = Credentials,
                    ApiKey = ApiKey,
                    EnlistInDistributedTransactions = EnlistInDistributedTransactions
                };
            }
            else
            {
                _inner = new EmbeddedDocumentStore
                {
                    DataDirectory = DataDirectory,
                    Conventions = Conventions,
                    Configuration = Configuration,
                    UseEmbeddedHttpServer = UseEmbeddedHttpServer,
                    RunInMemory = RunInMemory,
                    DefaultDatabase = DefaultDatabase,
                    ResourceManagerId = ResourceManagerId,
                    EnlistInDistributedTransactions = EnlistInDistributedTransactions
                };
            }

            _inner.SetListeners(Listeners);

			Configuration.PostInit();
            _inner.Initialize();	        

            return this;
        }

	    private void AssertValidConfiguration()
	    {
		    if(string.IsNullOrEmpty(Url) == false && UseEmbeddedHttpServer)
				throw new InvalidOperationException("You cannot set non empty Url and UseEmbeddedHttpServer = true at the same time. If you want to have HTTP access then you should use only UseEmbeddedHttpServer property, the listening port will be set according to configuration.");
	    }

        public Guid ResourceManagerId { get; set; }

	    public RavenDbServer ServerIfEmbedded
	    {
		    get
		    {
				var eds = Inner as EmbeddedDocumentStore;
				if (eds != null)
					return eds.Server;
				return null;
		    }
	    }

		/// <summary>
		/// Direct access to system database.
		/// </summary>
        public DocumentDatabase SystemDatabase
        {
            get
            {
                var eds = Inner as EmbeddedDocumentStore;
                if (eds != null)
                    return eds.SystemDatabase;
                return null;
            }
        }

		/// <summary>
		/// Direct access to database configured in 'DefaultDatabase' property. If property value is 'null' then 'SystemDatabase' is returned. 
		/// </summary>
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
            get; set;
        }

	    public string DataDirectory
	    {
		    get { return Configuration.DataDirectory; }
			set { Configuration.DataDirectory = value; }
	    }
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

	    public bool HasJsonRequestFactory
	    {
		    get
		    {
			    return Inner.HasJsonRequestFactory;
		    }
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

        public IFilesStore FilesStore
        {
            get
            {
                var eds = Inner as EmbeddedDocumentStore;
                if (eds != null)
                    return eds.FilesStore;
                return null;
            }
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

        public DocumentSessionListeners Listeners { get; private set; }

		public IAsyncReliableSubscriptions AsyncSubscriptions
		{
			get { return Inner.AsyncSubscriptions; }
		}

	    public IReliableSubscriptions Subscriptions
	    {
			get { return Inner.Subscriptions; }
	    }

        public void SetListeners(DocumentSessionListeners listeners)
        {
            this.Listeners = listeners;
        }

        ///<summary>
        /// Whatever we should also host an HTTP endpoint for the document database
        ///</summary>
        public bool UseEmbeddedHttpServer { get; set; }

		public bool RunInMemory
		{
			get { return Configuration.RunInMemory; }
			set { Configuration.RunInMemory = value; }
		}

        public IDocumentStore RegisterListener(IDocumentStoreListener listener)
        {
            Listeners.RegisterListener(listener);
            return this;
        }

        public IDocumentStore RegisterListener(IDocumentDeleteListener listener)
        {
            Listeners.RegisterListener(listener);
            return this;
        }


        public IDocumentStore RegisterListener(IDocumentConversionListener listener)
        {
            Listeners.RegisterListener(listener);
            return this;
        }

        public IDocumentStore RegisterListener(IDocumentQueryListener listener)
        {
            Listeners.RegisterListener(listener);
            return this;
        }


        public IDocumentStore RegisterListener(IDocumentConflictListener listener)
        {
            Listeners.RegisterListener(listener);
            return this;
        }

	    public void InitializeProfiling()
	    {
			_inner.InitializeProfiling();
	    }

	    public ProfilingInformation GetProfilingInformationFor(Guid id)
	    {
		    return _inner.GetProfilingInformationFor(id);
	    }
    }
}