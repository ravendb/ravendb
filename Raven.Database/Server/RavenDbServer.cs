//-----------------------------------------------------------------------
// <copyright file="RavenDbServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading.Tasks;
using Raven.Client.Document;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Embedded;
using Raven.Database.Server;
using Raven.Database.Server.RavenFS;
using Raven.Database.Server.WebApi;

namespace Raven.Server
{
	public class RavenDbServer : IDisposable
	{
	    private InMemoryRavenConfiguration configuration;
	    private IServerThingsForTests serverThingsForTests;
		private RavenDBOptions options;
	    private OwinHttpServer owinHttpServer;
        private DocumentStore documentStore;
	    private string url;

	    public RavenDbServer()
			: this(new RavenConfiguration())
		{}

		public RavenDbServer(InMemoryRavenConfiguration configuration)
		{
		    this.configuration = configuration;
		    documentStore = new DocumentStore();
		}

	    public InMemoryRavenConfiguration Configuration
	    {
	        get { return configuration; }
	        set { configuration = value; }
	    }

	    //TODO http://issues.hibernatingrhinos.com/issue/RavenDB-1451
		public DocumentDatabase SystemDatabase
		{
		    get
		    {
		        if (options == null)
		            return null;
		        return options.SystemDatabase;
		    }
		}

		//TODO http://issues.hibernatingrhinos.com/issue/RavenDB-1451
		public IServerThingsForTests Server
		{
			get { return serverThingsForTests; }
		}

	    public DocumentStore DocumentStore
	    {
            get { return documentStore; }
	    }

	    public bool RunInMemory
	    {
	        get { return configuration.RunInMemory; }
            set { configuration.RunInMemory = value; }
	    }

	    public RavenDbServer Initialize(Action<RavenDBOptions> configure = null)
	    {
            owinHttpServer = new OwinHttpServer(configuration, useHttpServer: UseEmbeddedHttpServer, configure: configure);
            options = owinHttpServer.Options;
            serverThingsForTests = new ServerThingsForTests(options);
	        documentStore.HttpMessageHandler = new OwinClientHandler(owinHttpServer.Invoke);
	        documentStore.Url = string.IsNullOrWhiteSpace(Url) ? "http://localhost" : Url;
	        documentStore.Initialize();
	        return this;
	    }

	    public RavenDBOptions Options
	    {
	        get { return options; }
	    }

        public string Url
        {
            get { return url; }
            set { url = value.EndsWith("/") ? value.Substring(0, value.Length - 1) : value; }
        }

	    ///<summary>
        /// Whatever we should also host an HTTP endpoint for the document database
        ///</summary>
        public bool UseEmbeddedHttpServer { get; set; }

	    public void Dispose()
	    {
		    if (documentStore != null)
			    documentStore.Dispose();

		    if (owinHttpServer != null)
			    owinHttpServer.Dispose();
	    }

		//TODO http://issues.hibernatingrhinos.com/issue/RavenDB-1451
		private class ServerThingsForTests : IServerThingsForTests
		{
			private readonly RavenDBOptions options;

			public ServerThingsForTests(RavenDBOptions options)
			{
				this.options = options;
			}

			public bool HasPendingRequests
			{
				get { return false; } //TODO DH: fix (copied from WebApiServer)
			}

			public int NumberOfRequests
			{
				get { return options.RequestManager.NumberOfRequests; }
			}

			public void ResetNumberOfRequests()
			{
				options.RequestManager.ResetNumberOfRequests();
			}

			public Task<DocumentDatabase> GetDatabaseInternal(string databaseName)
			{
				return options.DatabaseLandlord.GetDatabaseInternal(databaseName);
			}

            public Task<RavenFileSystem> GetRavenFileSystemInternal(string fileSystemName)
            {
                return options.FileSystemLandlord.GetFileSystemInternal(fileSystemName);
            }

			public RequestManager RequestManager { get { return options.RequestManager; } }
		}
	}

	//TODO http://issues.hibernatingrhinos.com/issue/RavenDB-1451
	public interface IServerThingsForTests
	{
		bool HasPendingRequests { get; }
		int NumberOfRequests { get; }
		void ResetNumberOfRequests();
		Task<DocumentDatabase> GetDatabaseInternal(string databaseName);
	    Task<RavenFileSystem> GetRavenFileSystemInternal(string fileSystemName);

		RequestManager RequestManager { get; }
	}
}