using Raven.Client.Document;

namespace Raven.Tests.Bundles.Versioning
{
	public class VersioningTest : RavenTest
	{
		protected readonly DocumentStore documentStore;

		public VersioningTest()
		{
			documentStore = CreateDocumentStore(8079);
		}

		protected DocumentStore CreateDocumentStore(int port)
		{
			var ravenDbServer = GetNewServer(activeBundles: "Versioning", port: port);
			var store = NewRemoteDocumentStore(ravenDbServer: ravenDbServer);

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
					Id = "Raven/Versioning/DefaultConfiguration",
					MaxRevisions = 5
				});
				session.SaveChanges();
			}

			return (DocumentStore)store;
		}
	}
}