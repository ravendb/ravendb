extern alias database;

using System;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Reflection;
using Raven.Bundles.Versioning.Data;
using Raven.Bundles.Versioning.Triggers;
using Raven.Client.Document;
using Raven.Server;

namespace Raven.Bundles.Tests.Versioning
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
			database::Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
			ravenDbServer = new RavenDbServer(
				new database::Raven.Database.Config.RavenConfiguration
				{
					Port = 8079,
					DataDirectory = path,
					RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
					Catalog =
						{
							Catalogs =
								{
									new AssemblyCatalog(typeof (VersioningPutTrigger).Assembly)
								}
						},
				});
			documentStore = new DocumentStore
			{
				Url = "http://localhost:8079"
			};
			documentStore.Initialize();
			using(var s = documentStore.OpenSession())
			{
				s.Store(new VersioningConfiguration
				{
					Exclude = true,
					Id = "Raven/Versioning/Users",
				});
				s.Store(new VersioningConfiguration
				{
					Exclude = true,
					Id = "Raven/Versioning/Comments",
				});
				s.Store(new VersioningConfiguration
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
			database::Raven.Database.Extensions.IOExtensions.DeleteDirectory(path);
		}
	}
}