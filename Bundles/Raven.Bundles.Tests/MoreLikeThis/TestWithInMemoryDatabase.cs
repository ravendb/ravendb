extern alias database;
using System;
using System.IO;
using System.Reflection;
using Raven.Client.Document;
using Raven.Server;

namespace Raven.Bundles.Tests.MoreLikeThis
{
	public class TestWithInMemoryDatabase : IDisposable
	{
		protected DocumentStore documentStore;
		string path;
		RavenDbServer ravenDbServer;

		protected TestWithInMemoryDatabase(Action<database::Raven.Database.Config.RavenConfiguration> configModifier)
		{
			var ravenConfiguration = new database::Raven.Database.Config.RavenConfiguration
			{
				Port = 8079,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
			};

			configModifier(ravenConfiguration);

			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(TestWithInMemoryDatabase)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);

			ravenConfiguration.DataDirectory = path;

			database::Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);

			ravenDbServer = new RavenDbServer(ravenConfiguration);

			documentStore = new DocumentStore
			{
				Url = "http://localhost:8079"
			};
			documentStore.Initialize();
		}

		public void Dispose()
		{
			documentStore.Dispose();
			ravenDbServer.Dispose();
			database::Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
		}
	}
}