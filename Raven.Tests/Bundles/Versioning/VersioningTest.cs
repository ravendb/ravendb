using System;
using System.IO;
using System.Reflection;
using Raven.Client.Document;
using Raven.Server;

namespace Raven.Tests.Bundles.Versioning
{
	public class VersioningTest : IDisposable
	{
		protected DocumentStore documentStore;
		private RavenDbServer ravenDbServer;
		private readonly string path;

		public VersioningTest()
		{
			path = Path.GetDirectoryName(Assembly.GetAssembly(typeof(Versioning)).CodeBase);
			path = Path.Combine(path, "TestDb").Substring(6);
			Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
			var cfg = new Raven.Database.Config.RavenConfiguration
			          	{
			          		Port = 8079,
			          		DataDirectory = path,
			          		RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
			          		Settings =
			          			{
			          				{"Raven/ActiveBundles", "Versioning"}
			          			}
			          	};
			cfg.PostInit();
			ravenDbServer = new RavenDbServer(cfg);
			documentStore = new DocumentStore
			{
				Url = "http://localhost:8079"
			};
			documentStore.Initialize();
			using(var s = documentStore.OpenSession())
			{
				s.Store(new Raven.Bundles.Versioning.Data.VersioningConfiguration
				{
					Exclude = true,
					Id = "Raven/Versioning/Users",
				});
				s.Store(new Raven.Bundles.Versioning.Data.VersioningConfiguration
				{
					Exclude = true,
					Id = "Raven/Versioning/Comments",
				});
				s.Store(new Raven.Bundles.Versioning.Data.VersioningConfiguration
				{
					Exclude = false,
					Id = "Raven/Versioning/DefaultConfiguration",
					MaxRevisions = 5
				});
				s.SaveChanges();
			}
		}

		public void Dispose()
		{
			documentStore.Dispose();
			ravenDbServer.Dispose();
			Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
		}
	}
}