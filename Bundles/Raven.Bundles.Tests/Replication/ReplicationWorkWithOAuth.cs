extern alias database;
using System.ComponentModel.Composition.Hosting;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Bundles.Authentication;
using Xunit;

namespace Raven.Bundles.Tests.Replication
{
	public class ReplicationWorkWithOAuth : ReplicationBase
	{
		protected override void ConfigureServer(database.Raven.Database.Config.RavenConfiguration serverConfiguration)
		{
			serverConfiguration.AuthenticationMode = "oauth";
			serverConfiguration.AnonymousUserAccessMode = database::Raven.Database.Server.AnonymousUserAccessMode.None;
			serverConfiguration.OAuthTokenCertificate = database::Raven.Database.Config.CertGenerator.GenerateNewCertificate("RavenDB.Test");

			var authenticationCatalog = new AssemblyCatalog(typeof(AuthenticationUser).Assembly);
			serverConfiguration.Catalog.Catalogs.Add(authenticationCatalog);
		}

		[Fact]
		public void CanReplicateDocumentWithOAuth()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			TellFirstInstanceToReplicateToSecondInstance();

			using (var session = store1.OpenSession())
			{
				session.Store(new Item());
				session.SaveChanges();
			}

			JsonDocument item = null;
			for (int i = 0; i < RetriesCount; i++)
			{
				item = store2.DatabaseCommands.Get("item/1");
				if (item != null)
					break;
				Thread.Sleep(100);
			}

			Assert.NotNull(item);
		}

		public class Item
		{
		}
	}
}