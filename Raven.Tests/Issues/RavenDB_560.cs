using System;
using Raven.Bundles.Replication.Tasks;
using System.Linq;
using System.Threading;
using Raven.Abstractions.Data;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Extensions;
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
		public void ClientShouldGetInformationFromSecondaryServerThatItsPrimaryServerMightBeUp()
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

			store1.DatabaseCommands.CreateDatabase(
				new DatabaseDocument
				{
					Id = "Northwind",
					Settings = {{"Raven/ActiveBundles", "replication"}, {"Raven/DataDir", @"~\D1\N"}}
				});

			store2.DatabaseCommands.CreateDatabase(
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
				replicationInformerForDatabase
					.UpdateReplicationInformationIfNeeded((ServerClient) store.DatabaseCommands)
					.Wait();

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

				var replicationTask =
					server2.Server.GetDatabaseInternal("Northwind").Result.StartupTasks.OfType<ReplicationTask>().First();
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
		public void ClientShouldGetInformationFromSecondaryServerThatItsPrimaryServerMightBeUpAsync()
		{
			server1 = this.CreateServer(8113, "D1");
			server2 = this.CreateServer(8114, "D2");

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

			store1.DatabaseCommands.CreateDatabase(
				new DatabaseDocument
				{
					Id = "Northwind",
					Settings = {{"Raven/ActiveBundles", "replication"}, {"Raven/DataDir", @"~\D1\N"}}
				});

			store2.DatabaseCommands.CreateDatabase(
				new DatabaseDocument
				{
					Id = "Northwind",
					Settings = {{"Raven/ActiveBundles", "replication"}, {"Raven/DataDir", @"~\D2\N"}}
				});

			var db1Url = store1.Url + "/databases/Northwind";
			var db2Url = store2.Url + "/databases/Northwind";

			this.SetupReplication(store1.DatabaseCommands, db2Url);

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
				replicationInformerForDatabase
					.UpdateReplicationInformationIfNeeded((ServerClient) store.DatabaseCommands)
					.Wait();

				Assert.NotEmpty(replicationInformerForDatabase.ReplicationDestinations);

				using (var session = store.OpenAsyncSession())
				{
					session.Store(new Item());
					session.SaveChangesAsync().Wait();
				}

				this.WaitForDocument(store2.DatabaseCommands, "items/1");

				Assert.Equal(0, replicationInformerForDatabase.GetFailureCount(db1Url));

				this.StopServer(server1);

				// Fail few times so we will be sure that client does not try its primary url
				for (int i = 0; i < 2; i++)
				{
					Assert.NotNull(store.AsyncDatabaseCommands.GetAsync("items/1").Result);
				}
				Assert.True(replicationInformerForDatabase.GetFailureCount(db1Url) > 0);

				var replicationTask =
					server2.Server.GetDatabaseInternal("Northwind").Result.StartupTasks.OfType<ReplicationTask>().First();
				replicationTask.Heartbeats.Clear();

				server1 = this.StartServer(server1);

				Assert.NotNull(store1.AsyncDatabaseCommands.GetAsync("items/1").Result);

				while (true)
				{
					DateTime time;
					if (replicationTask.Heartbeats.TryGetValue(db1Url.Replace("localhost", Environment.MachineName.ToLowerInvariant()), out time))
					{
						break;
					}
					Thread.Sleep(100);
				}

				Assert.NotNull(store.AsyncDatabaseCommands.GetAsync("items/1").Result);

				Assert.True(replicationInformerForDatabase.GetFailureCount(db1Url) > 0);

				Assert.NotNull(store.AsyncDatabaseCommands.GetAsync("items/1").Result);

				Assert.Equal(0, replicationInformerForDatabase.GetFailureCount(db1Url));
			}
		}

		private RavenDbServer CreateServer(int port, string dataDirectory, bool removeDataDirectory = true)
		{
			Raven.Database.Server.NonAdminHttp.EnsureCanListenToWhenInNonAdminContext(port);

			var serverConfiguration = new Raven.Database.Config.RavenConfiguration
			{
				Settings = { { "Raven/ActiveBundles", "replication" } },
				AnonymousUserAccessMode = Raven.Database.Server.AnonymousUserAccessMode.All,
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
