using System;
using System.Collections.Generic;
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
		private int dbCount;
		private readonly string testAssemblyPath;
		private readonly List<string> dbDirectories = new List<string>();

		public VersioningTest()
		{
			testAssemblyPath = Path.GetDirectoryName(Assembly.GetAssembly(typeof(Versioning)).CodeBase);

			ravenDbServer = CreateRavenDbServer(port: 8079);
			documentStore = CreateDocumentStore(port: 8079);
		}

		public void Dispose()
		{
			documentStore.Dispose();
			ravenDbServer.Dispose();
			dbDirectories.ForEach(Database.Extensions.IOExtensions.DeleteDirectory);
		}

		protected RavenDbServer CreateRavenDbServer(int port)
		{
			var path = Path.Combine(testAssemblyPath, "TestDb" + (++dbCount)).Substring(6);
			Database.Extensions.IOExtensions.DeleteDirectory(path);
			dbDirectories.Add(path);

			var cfg = new Database.Config.RavenConfiguration
			{
				Port = port,
				DataDirectory = path,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
				Settings =
					{
						{"Raven/ActiveBundles", "Versioning"}
					}
			};
			cfg.PostInit();
			return new RavenDbServer(cfg);
		}

		protected static DocumentStore CreateDocumentStore(int port)
		{
			var documentStore = new DocumentStore
			{
				Url = "http://localhost:" + port
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
			return documentStore;
		}
	}
}