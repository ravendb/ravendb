extern alias database;
using System.ComponentModel.Composition.Hosting;
using System.IO;
using System.Net;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Bundles.Authentication;
using Raven.Json.Linq;
using Xunit;

namespace Raven.Bundles.Tests.Replication.Async
{
	public class ReplicationWithOAuth : ReplicationBase
	{
		protected override void ConfigureServer(database::Raven.Database.Config.RavenConfiguration serverConfiguration)
		{
			serverConfiguration.AnonymousUserAccessMode = database::Raven.Database.Server.AnonymousUserAccessMode.None;

			serverConfiguration.Catalog.Catalogs.Add(new AssemblyCatalog(typeof(AuthenticationUser).Assembly));
		}


		protected override void ConfigureStore(Client.Document.DocumentStore documentStore)
		{
			documentStore.Credentials = new NetworkCredential("Ayende", "abc");
			base.ConfigureStore(documentStore);
		}

		protected override void SetupDestination(Abstractions.Replication.ReplicationDestination replicationDestination)
		{
			replicationDestination.Username = "Ayende";
			replicationDestination.Password = "abc";
		}

		[Fact]
		public void CanReplicateDocumentWithOAuth()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			foreach (var server in servers)
			{
				var writer = new StringWriter();
				store1.Conventions.CreateSerializer().Serialize(writer, new AuthenticationUser
				{
					Name = "Ayende",
					Id = "Raven/Users/Ayende",
					AllowedDatabases = new[] {"*"}
				}.SetPassword("abc"));

				server.Database.Put("Raven/Users/Ayende", null, RavenJObject.Parse(writer.GetStringBuilder().ToString()), new RavenJObject(), null);
			}

			TellFirstInstanceToReplicateToSecondInstance();

			using (var session = store1.OpenAsyncSession())
			{
				session.Store(new Item());
				session.SaveChangesAsync().Wait();
			}

			JsonDocument item = null;
			for (int i = 0; i < RetriesCount; i++)
			{
				item = store2.DatabaseCommands.Get("items/1");
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