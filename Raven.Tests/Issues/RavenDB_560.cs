using System;
using System.Threading.Tasks;
using Raven.Bundles.Replication.Tasks;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Server;
using Raven.Tests.Bundles.Replication;
using Xunit;

namespace Raven.Tests.Issues
{
	public class RavenDB_560 : ReplicationBase
	{
		private class Item
		{
		}
		
		private RavenDbServer server1;
		private RavenDbServer server2;
		private DocumentStore store1;
		private DocumentStore store2;

		[Fact]
		public async Task ClientShouldGetInformationFromSecondaryServerThatItsPrimaryServerMightBeUp()
		{
			server1 = CreateServer(8111, "D1");
			server2 = CreateServer(8112, "D2");

			store1 = new DocumentStore
			{
				DefaultDatabase = "Northwind",
				Url = "http://localhost:8111"
			};

			store1.Initialize();

			store2 = new DocumentStore
			{
				DefaultDatabase = "Northwind",
				Url = "http://localhost:8112"
			};

			store2.Initialize();

			store1.DatabaseCommands.GlobalAdmin.CreateDatabase(
				new DatabaseDocument
				{
					Id = "Northwind",
					Settings = {{"Raven/ActiveBundles", "replication"}, {"Raven/DataDir", @"~\D1\N"}}
				});

			store2.DatabaseCommands.GlobalAdmin.CreateDatabase(
				new DatabaseDocument
				{
					Id = "Northwind",
					Settings = {{"Raven/ActiveBundles", "replication"}, {"Raven/DataDir", @"~\D2\N"}}
				});

			var db1Url = store1.Url + "/databases/Northwind";
			var db2Url = store2.Url + "/databases/Northwind";

			SetupReplication(store1.DatabaseCommands, db2Url);

			using (var store = new DocumentStore
			{
				DefaultDatabase = "Northwind",
				Url = store1.Url,
				Conventions =
				{
					FailoverBehavior = FailoverBehavior.AllowReadsFromSecondariesAndWritesToSecondaries
				}
			})
			{

				store.Initialize();
				var replicationInformerForDatabase = store.GetReplicationInformerForDatabase("Northwind");
				await replicationInformerForDatabase.UpdateReplicationInformationIfNeeded((ServerClient) store.DatabaseCommands);

				Assert.NotEmpty(replicationInformerForDatabase.ReplicationDestinations);

				using (var session = store.OpenSession())
				{
					session.Store(new Item());
					session.SaveChanges();
				}

				WaitForDocument(store2.DatabaseCommands, "items/1");

				Assert.Equal(0, replicationInformerForDatabase.GetFailureCount(db1Url));

				StopServer(server1);

				// Fail few times so we will be sure that client does not try its primary url
				for (int i = 0; i < 2; i++)
				{
					Assert.NotNull(store.DatabaseCommands.Get("items/1"));
				}

				Assert.True(replicationInformerForDatabase.GetFailureCount(db1Url) > 0);

				var replicationTask = (await server2.Server.GetDatabaseInternal("Northwind")).StartupTasks.OfType<ReplicationTask>().First();
				replicationTask.Heartbeats.Clear();

				server1 = StartServer(server1);

				Assert.NotNull(store1.DatabaseCommands.Get("items/1"));


				while (true)
				{
					DateTime time;
					if (replicationTask.Heartbeats.TryGetValue(db1Url.Replace("localhost", Environment.MachineName.ToLowerInvariant()), out time))
					{
						break;
					}
					Thread.Sleep(100);
				}

				Assert.True(replicationInformerForDatabase.GetFailureCount(db1Url) > 0);
				
				Assert.NotNull(store.DatabaseCommands.Get("items/1"));

				Assert.True(replicationInformerForDatabase.GetFailureCount(db1Url) > 0);

				Assert.NotNull(store.DatabaseCommands.Get("items/1"));

				Assert.Equal(0, replicationInformerForDatabase.GetFailureCount(db1Url));
			}
		}

		[Fact]
		public async Task ClientShouldGetInformationFromSecondaryServerThatItsPrimaryServerMightBeUpAsync()
		{
			server1 = CreateServer(8113, "D1");
			server2 = CreateServer(8114, "D2");

			store1 = new DocumentStore
			{
				DefaultDatabase = "Northwind",
				Url = "http://localhost:8113"
			};
			store1.Initialize();

			store2 = new DocumentStore
			{
				DefaultDatabase = "Northwind",
				Url = "http://localhost:8114"
			};
			store2.Initialize();

			store1.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
			{
				Id = "Northwind",
				Settings = {{"Raven/ActiveBundles", "replication"}, {"Raven/DataDir", @"~\D1\N"}}
			});

			store2.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
			{
				Id = "Northwind",
				Settings = {{"Raven/ActiveBundles", "replication"}, {"Raven/DataDir", @"~\D2\N"}}
			});

			var db1Url = store1.Url + "/databases/Northwind";
			var db2Url = store2.Url + "/databases/Northwind";

			SetupReplication(store1.DatabaseCommands, db2Url);

			using (var store = new DocumentStore
			{
				DefaultDatabase = "Northwind",
				Url = store1.Url,
				Conventions =
				{
					FailoverBehavior = FailoverBehavior.AllowReadsFromSecondariesAndWritesToSecondaries
				}
			})
			{

				store.Initialize();
				var replicationInformerForDatabase = store.GetReplicationInformerForDatabase("Northwind");
				await replicationInformerForDatabase.UpdateReplicationInformationIfNeeded((ServerClient) store.DatabaseCommands);

				Assert.NotEmpty(replicationInformerForDatabase.ReplicationDestinations);

				using (var session = store.OpenAsyncSession())
				{
					await session.StoreAsync(new Item());
					await session.SaveChangesAsync();
				}

				WaitForDocument(store2.DatabaseCommands, "items/1");

				Assert.Equal(0, replicationInformerForDatabase.GetFailureCount(db1Url));

				StopServer(server1);

				// Fail few times so we will be sure that client does not try its primary url
				for (int i = 0; i < 2; i++)
				{
					Assert.NotNull(await store.AsyncDatabaseCommands.GetAsync("items/1"));
				}
				Assert.True(replicationInformerForDatabase.GetFailureCount(db1Url) > 0);

				var database = await server2.Server.GetDatabaseInternal("Northwind");
				var replicationTask = database.StartupTasks.OfType<ReplicationTask>().First();
				replicationTask.Heartbeats.Clear();

				server1 = StartServer(server1);

				Assert.NotNull(await store1.AsyncDatabaseCommands.GetAsync("items/1"));

				while (true)
				{
					DateTime time;
					if (replicationTask.Heartbeats.TryGetValue(db1Url.Replace("localhost", Environment.MachineName.ToLowerInvariant()), out time))
					{
						break;
					}
					Thread.Sleep(100);
				}

				Assert.NotNull(await store.AsyncDatabaseCommands.GetAsync("items/1"));
				Assert.True(replicationInformerForDatabase.GetFailureCount(db1Url) > 0);

				Assert.NotNull(await store.AsyncDatabaseCommands.GetAsync("items/1"));
				Assert.Equal(0, replicationInformerForDatabase.GetFailureCount(db1Url));
			}
		}

		private RavenDbServer CreateServer(int port, string dataDirectory, bool removeDataDirectory = true)
		{
			Database.Server.NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port);

			var serverConfiguration = new Database.Config.RavenConfiguration
			{
				Settings = { { "Raven/ActiveBundles", "replication" } },
                AnonymousUserAccessMode = Raven.Database.Server.AnonymousUserAccessMode.Admin,
				DataDirectory = dataDirectory,
				RunInUnreliableYetFastModeThatIsNotSuitableForProduction = true,
				RunInMemory = false,
				Port = port,
				DefaultStorageTypeName = "esent"
			};

			if (removeDataDirectory)
				IOExtensions.DeleteDirectory(serverConfiguration.DataDirectory);

			var server = new RavenDbServer(serverConfiguration);
			serverConfiguration.PostInit();

			return server;
		}

		private void StopServer(RavenDbServer server)
		{
			server.Dispose();
		}

		private RavenDbServer StartServer(RavenDbServer server)
		{
			return this.CreateServer(server.Database.Configuration.Port, server.Database.Configuration.DataDirectory, false);
		}

		public override void Dispose()
		{
			if (server1 != null)
			{
				server1.Dispose();
				IOExtensions.DeleteDirectory(server1.Database.Configuration.DataDirectory);
			}

			if (server2 != null)
			{
				server2.Dispose();
				IOExtensions.DeleteDirectory(server2.Database.Configuration.DataDirectory);
			}

			if (store1 != null)
			{
				store1.Dispose();
			}

			if (store2 != null)
			{
				store2.Dispose();
			}
		}
	}
}