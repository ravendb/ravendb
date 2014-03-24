//-----------------------------------------------------------------------
// <copyright file="RavenDbServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading.Tasks;
using Raven.Client;
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
	    private readonly InMemoryRavenConfiguration configuration;
	    private IServerThingsForTests serverThingsForTests;
		private RavenDBOptions options;
	    private OwinHttpServer owinHttpServer;
        private IDocumentStore documentStore;

	    public RavenDbServer()
			: this(new RavenConfiguration())
		{}

		public RavenDbServer(InMemoryRavenConfiguration configuration)
		{
		    this.configuration = configuration;
		   
		}

		//TODO http://issues.hibernatingrhinos.com/issue/RavenDB-1451
		public DocumentDatabase SystemDatabase
		{
			get { return options.SystemDatabase; }
		}

		//TODO http://issues.hibernatingrhinos.com/issue/RavenDB-1451
		public IServerThingsForTests Server
		{
			get { return serverThingsForTests; }
		}

	    public IDocumentStore DocumentStore
	    {
            get { return documentStore; }
	    }

	    public bool RunInMemory
	    {
	        get { return configuration.RunInMemory; }
            set { configuration.RunInMemory = value; }
	    }

	    public RavenDbServer Initialize()
	    {
            owinHttpServer = new OwinHttpServer(configuration, useHttpServer: UseEmbeddedHttpServer);
            options = owinHttpServer.Options;
            serverThingsForTests = new ServerThingsForTests(options);
            documentStore = new DocumentStore
            {
                HttpMessageHandler = new OwinClientHandler(owinHttpServer.Invoke),
                Url = "http://localhost"
            }.Initialize();
	        return this;
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