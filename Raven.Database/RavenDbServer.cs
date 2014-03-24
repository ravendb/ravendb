//-----------------------------------------------------------------------
// <copyright file="RavenDbServer.cs" company="Hibernating Rhinos LTD">
//     Copyright (c) Hibernating Rhinos LTD. All rights reserved.
// </copyright>
//-----------------------------------------------------------------------
using System;
using System.Threading.Tasks;
using Raven.Database;
using Raven.Database.Config;
using Raven.Database.Server;
using Raven.Database.Server.RavenFS;
using Raven.Database.Server.WebApi;
using Raven.Database.Util;

namespace Raven.Server
{
	public class RavenDbServer : IDisposable
	{
		private readonly IServerThingsForTests serverThingsForTests;
		private readonly CompositeDisposable compositeDisposable = new CompositeDisposable();
		private readonly RavenDBOptions options;

		public RavenDbServer()
			: this(new RavenConfiguration())
		{}

		public RavenDbServer(InMemoryRavenConfiguration configuration)
		{
			var owinHttpServer = new OwinHttpServer(configuration);
			options = owinHttpServer.Options;
			compositeDisposable.Add(owinHttpServer);
			serverThingsForTests = new ServerThingsForTests(options);
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

		public void Dispose()
		{
			compositeDisposable.Dispose();
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