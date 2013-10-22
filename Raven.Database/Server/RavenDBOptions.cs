using System;
using Raven.Database.Config;
using Raven.Database.Server.Security;
using Raven.Database.Server.Tenancy;
using Raven.Database.Server.WebApi;

namespace Raven.Database.Server
{
	public sealed class RavenDBOptions : IDisposable
	{
		private readonly InMemoryRavenConfiguration configuration;
		private readonly DatabasesLandlord databasesLandlord;
		private readonly MixedModeRequestAuthorizer mixedModeRequestAuthorizer;
		private readonly DocumentDatabase systemDatabase;

		public RavenDBOptions(InMemoryRavenConfiguration configuration)
		{
			this.configuration = configuration;
			//TODO DH: should we do HttpEndpointRegistration.RegisterHttpEndpointTarget(); here?
			systemDatabase = new DocumentDatabase(configuration);
			try
			{
				systemDatabase.SpinBackgroundWorkers();
				databasesLandlord = new DatabasesLandlord(systemDatabase);
				mixedModeRequestAuthorizer = new MixedModeRequestAuthorizer();
				mixedModeRequestAuthorizer.Initialize(systemDatabase,
					new RavenServer(databasesLandlord.SystemDatabase, configuration));
			}
			catch
			{
				systemDatabase.Dispose();
				throw;
			}
		}

		public InMemoryRavenConfiguration Configuration
		{
			get { return configuration; }
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

		public void Dispose()
		{
			databasesLandlord.Dispose();
			systemDatabase.Dispose();
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