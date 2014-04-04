using System;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

using Raven.Abstractions;
using Raven.Abstractions.Data;
using Raven.Bundles.Replication.Tasks;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Server;
using Raven.Tests.Common;

using Xunit;

namespace Raven.SlowTests.Issues
{
	public class RavenDB_560 : ReplicationBase
	{
		private class Item
		{
		}

		private readonly string server1DataDir;
		private readonly string server2DataDir;
		private RavenDbServer server1;
		private RavenDbServer server2;
		private DocumentStore store1;
		private DocumentStore store2;

		public RavenDB_560()
		{
			server1DataDir = NewDataPath("D1");
			server2DataDir = NewDataPath("D2");
			server1 = CreateServer(8001, server1DataDir);
			server2 = CreateServer(8002, server2DataDir);


			store1 = new DocumentStore
			{
				DefaultDatabase = "Northwind",
				Url = "http://" + Environment.MachineName + ":8001",
			};
			store1.Initialize(false);

			store2 = new DocumentStore
			{
				DefaultDatabase = "Northwind",
				Url = "http://" + Environment.MachineName + ":8002"
			};
			store2.Initialize(false);


			store1.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
			{
				Id = "Northwind",
				Settings = { { "Raven/ActiveBundles", "replication" }, { "Raven/DataDir", @"~\Databases1\Northwind" } }
			});

			store2.DatabaseCommands.GlobalAdmin.CreateDatabase(new DatabaseDocument
			{
				Id = "Northwind",
				Settings = { { "Raven/ActiveBundles", "replication" }, { "Raven/DataDir", @"~\Databases2\Northwind" } }
			});
		}

		[Fact]
		public async Task ClientShouldGetInformationFromSecondaryServerThatItsPrimaryServerMightBeUp()
		{
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

				server1 = CreateServer(8001, server1DataDir);

				Assert.NotNull(store1.DatabaseCommands.Get("items/1"));

				while (true)
				{
					var name = db1Url.Replace("localhost", Environment.MachineName.ToLowerInvariant())
					                 .Replace(".fiddler", "");
					DateTime time;
					if (replicationTask.Heartbeats.TryGetValue(name, out time))
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

				server1 = CreateServer(8001, server1DataDir);

				Assert.NotNull(await store1.AsyncDatabaseCommands.GetAsync("items/1"));

				while (true)
				{
					var name = db1Url.Replace("localhost", Environment.MachineName.ToLowerInvariant())
					                 .Replace(".fiddler", "");
					DateTime time;
					if (replicationTask.Heartbeats.TryGetValue(name, out time))
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

		private RavenDbServer CreateServer(int port, string dataDirectory)
		{
			return GetNewServer(port, dataDirectory, activeBundles: "replication", requestedStorage: "esent", runInMemory: false);
		}

        private void StopServer(RavenDbServer server)
        {
            server.Dispose();
            SystemTime.UtcDateTime = () => DateTime.UtcNow.AddSeconds(4); // just to prevent Raven-Client-Primary-Server-LastCheck to have same second
        }

		public override void Dispose()
		{
			base.Dispose();

			if (store1 != null)
				store1.Dispose();

			if (store2 != null)
				store2.Dispose();
		}
	}
}