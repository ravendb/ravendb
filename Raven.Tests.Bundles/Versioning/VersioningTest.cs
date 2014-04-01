using System.Runtime.CompilerServices;

using Raven.Client.Document;
using Raven.Tests.Common;

namespace Raven.Tests.Bundles.Versioning
{
	public class VersioningTest : RavenTest
	{
		protected readonly DocumentStore documentStore;

		public VersioningTest()
		{
			documentStore = CreateDocumentStore(8079);
		}

		protected DocumentStore CreateDocumentStore(int port, [CallerMemberName]string databaseName = null)
		{
			var ravenDbServer = GetNewServer(activeBundles: "Versioning", port: port, databaseName: databaseName);
			var store = NewRemoteDocumentStore(ravenDbServer: ravenDbServer, databaseName: databaseName);

			using (var session = store.OpenSession())
			{
				session.Store(new Raven.Bundles.Versioning.Data.VersioningConfiguration
				{
					Exclude = true,
					Id = "Raven/Versioning/Users",
				});
				session.Store(new Raven.Bundles.Versioning.Data.VersioningConfiguration
				{
					Exclude = true,
					Id = "Raven/Versioning/Comments",
				});
				session.Store(new Raven.Bundles.Versioning.Data.VersioningConfiguration
				{
					Exclude = false,
					Id = "Raven/Versioning/Products",
					ExcludeUnlessExplicit = true,
				});
                session.Store(new Raven.Bundles.Versioning.Data.VersioningConfiguration
				{
					Exclude = false,
					Id = "Raven/Versioning/DefaultConfiguration",
					MaxRevisions = 5
				});
				session.SaveChanges();
			}

			return (DocumentStore)store;
		}
	}
}