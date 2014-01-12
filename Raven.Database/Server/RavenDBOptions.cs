using System;
using System.Threading.Tasks;
using Raven.Database.Config;
using Raven.Database.Server.Connections;
using Raven.Database.Server.RavenFS;
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
		private readonly Task<RavenFileSystem> fileSystem;

		public RavenDBOptions(InMemoryRavenConfiguration configuration)
		{
			if (configuration == null)
			{
				throw new ArgumentNullException("configuration");
			}
			systemDatabase = new DocumentDatabase(configuration);
			try
			{
				HttpEndpointRegistration.RegisterHttpEndpointTarget();
				systemDatabase.SpinBackgroundWorkers();
				var transportState = systemDatabase.TransportState;
				fileSystem = Task.Run(() => new RavenFileSystem(configuration, transportState));
				databasesLandlord = new DatabasesLandlord(systemDatabase);
				requestManager = new RequestManager(databasesLandlord);
				mixedModeRequestAuthorizer = new MixedModeRequestAuthorizer();
				mixedModeRequestAuthorizer.Initialize(systemDatabase, new RavenServer(databasesLandlord.SystemDatabase, configuration));
			}
			catch
			{
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

		public DatabasesLandlord Landlord
		{
			get { return databasesLandlord; }
		}

		public RequestManager RequestManager
		{
			get { return requestManager; }
		}

		public RavenFileSystem FileSystem
		{
			get { return fileSystem.Result; }
		}

		public void Dispose()
		{
			mixedModeRequestAuthorizer.Dispose();
			databasesLandlord.Dispose();
			systemDatabase.Dispose();
			requestManager.Dispose();
			FileSystem.Dispose();
			fileSystem.Dispose();
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