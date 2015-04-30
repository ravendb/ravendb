using System;
using System.Collections.Generic;

using Raven.Abstractions.Extensions;
using Raven.Abstractions.Logging;
using Raven.Abstractions.Util.Streams;
using Raven.Database.Config;
using Raven.Database.Raft;
using Raven.Database.Server.Connections;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi;

namespace Raven.Database.Server
{
	public sealed class RavenDBOptions : IDisposable
	{
		private readonly DatabasesLandlord databasesLandlord;
		private readonly MixedModeRequestAuthorizer mixedModeRequestAuthorizer;
		private readonly DocumentDatabase systemDatabase;
		private readonly RequestManager requestManager;
	    private readonly FileSystemsLandlord fileSystemLandlord;
		private readonly CountersLandlord countersLandlord;
		private readonly WebSocketBufferPool webSocketBufferPool;

		private bool preventDisposing;

		public RavenDBOptions(InMemoryRavenConfiguration configuration, DocumentDatabase db = null)
		{
			if (configuration == null)
				throw new ArgumentNullException("configuration");
			
			try
			{
				HttpEndpointRegistration.RegisterHttpEndpointTarget();
			    HttpEndpointRegistration.RegisterAdminLogsTarget();
				if (db == null)
				{
					configuration.UpdateDataDirForLegacySystemDb();
					systemDatabase = new DocumentDatabase(configuration, null);
					systemDatabase.SpinBackgroundWorkers();
				}
				else
				{
					systemDatabase = db;
				}
			    fileSystemLandlord = new FileSystemsLandlord(systemDatabase);
				databasesLandlord = new DatabasesLandlord(systemDatabase);
				countersLandlord = new CountersLandlord(systemDatabase);
				requestManager = new RequestManager(databasesLandlord);
				ClusterManager = new Reference<ClusterManager>();
				mixedModeRequestAuthorizer = new MixedModeRequestAuthorizer();
				webSocketBufferPool = new WebSocketBufferPool(configuration.WebSockets.InitialBufferPoolSize);
				mixedModeRequestAuthorizer.Initialize(systemDatabase, new RavenServer(databasesLandlord.SystemDatabase, configuration));
			}
			catch
			{
				if (systemDatabase != null)
					systemDatabase.Dispose();
				throw;
			}
		}

		public DocumentDatabase SystemDatabase
		{
			get { return systemDatabase; }
		}

		public MixedModeRequestAuthorizer MixedModeRequestAuthorizer
		{
			get { return mixedModeRequestAuthorizer; }
		}

		public DatabasesLandlord DatabaseLandlord
		{
			get { return databasesLandlord; }
		}
	    public FileSystemsLandlord FileSystemLandlord
	    {
	        get { return fileSystemLandlord; }
	    }

		public CountersLandlord CountersLandlord
		{
			get { return countersLandlord; }
		}

	    public RequestManager RequestManager
		{
			get { return requestManager; }
		}

		public Reference<ClusterManager> ClusterManager { get; private set; }

		public WebSocketBufferPool WebSocketBufferPool
		{
			get { return webSocketBufferPool; }
		}

		public void Dispose()
		{
			if(preventDisposing)
				return;

		    var toDispose = new List<IDisposable>
		                    {
		                        mixedModeRequestAuthorizer, 
                                databasesLandlord, 
                                fileSystemLandlord,
                                systemDatabase, 
                                LogManager.GetTarget<AdminLogsTarget>(),
                                requestManager,
                                countersLandlord,
								ClusterManager.Value
		                    };

            var errors = new List<Exception>();

		    foreach (var disposable in toDispose)
		    {
                try
                {
                    if (disposable != null)
                        disposable.Dispose();
                }
                catch (Exception e)
                {
                    errors.Add(e);
                }
		    }

			if (errors.Count != 0)
                throw new AggregateException(errors);
		}

		public IDisposable PreventDispose()
		{
			preventDisposing = true;

			return new DisposableAction(() => preventDisposing = false);
		}

		private class RavenServer : IRavenServer
		{
			private readonly InMemoryRavenConfiguration systemConfiguration;
			private readonly DocumentDatabase systemDatabase;

			public RavenServer(DocumentDatabase systemDatabase, InMemoryRavenConfiguration systemConfiguration)
			{
				this.systemDatabase = systemDatabase;
				this.systemConfiguration = systemConfiguration;
			}

			public DocumentDatabase SystemDatabase
			{
				get { return systemDatabase; }
			}

			public InMemoryRavenConfiguration SystemConfiguration
			{
				get { return systemConfiguration; }
			}
		}
	}
}