// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1041.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Threading.Tasks;
using Raven.Client.Connection;
using Raven.Client.Document;
using Raven.Client.Extensions;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Bundles.Replication;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1041 : ReplicationBase
	{
	    public RavenDB_1041()
	    {
			IOExtensions.DeleteDirectory("Database #0");
			IOExtensions.DeleteDirectory("Database #1");
			IOExtensions.DeleteDirectory("Database #2");
	    }
		class ReplicatedItem
		{
			public string Id { get; set; }
		}

        private const string DatabaseName = "RavenDB_1041";

		[Fact]
		public async Task CanWaitForReplication()
		{
			var store1 = CreateStore(requestedStorageType: "esent", databaseName: DatabaseName);
            var store2 = CreateStore(requestedStorageType: "esent", databaseName: DatabaseName);
            var store3 = CreateStore(requestedStorageType: "esent", databaseName: DatabaseName);

			SetupReplication(store1.DatabaseCommands, store2, store3);

			using (var session = store1.OpenSession(DatabaseName))
			{
				session.Store(new ReplicatedItem { Id = "Replicated/1" });

				session.SaveChanges();
			}

		    await ((DocumentStore)store1).Replication.WaitAsync(database: DatabaseName);

			Assert.NotNull(store2.DatabaseCommands.ForDatabase(DatabaseName).Get("Replicated/1"));
			Assert.NotNull(store3.DatabaseCommands.ForDatabase(DatabaseName).Get("Replicated/1"));
		}

		[Fact]
		public async Task CanWaitForReplicationOfParticularEtag()
		{
			var store1 = CreateStore(requestedStorageType:"esent");
            var store2 = CreateStore(requestedStorageType: "esent");
            var store3 = CreateStore(requestedStorageType: "esent");

			SetupReplication(store1.DatabaseCommands, store2, store3);

			var putResult = store1.DatabaseCommands.Put("Replicated/1", null, new RavenJObject(), new RavenJObject());
			var putResult2 = store1.DatabaseCommands.Put("Replicated/2", null, new RavenJObject(), new RavenJObject());

		    await ((DocumentStore)store1).Replication.WaitAsync(putResult.ETag);

			Assert.NotNull(store2.DatabaseCommands.Get("Replicated/1"));
			Assert.NotNull(store3.DatabaseCommands.Get("Replicated/1"));

			((DocumentStore)store1).Replication.WaitAsync(putResult2.ETag).Wait();

			Assert.NotNull(store2.DatabaseCommands.Get("Replicated/2"));
			Assert.NotNull(store3.DatabaseCommands.Get("Replicated/2"));
		}

		[Fact]
		public async Task CanWaitForReplicationInAsyncManner()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();
			var store3 = CreateStore();

			SetupReplication(store1.DatabaseCommands, store2, store3);

			using (var session = store1.OpenSession())
			{
				session.Store(new ReplicatedItem { Id = "Replicated/1" });

				session.SaveChanges();
			}

		    await store1.Replication.WaitAsync(timeout: TimeSpan.FromSeconds(10));

			Assert.NotNull(store2.DatabaseCommands.Get("Replicated/1"));
			Assert.NotNull(store3.DatabaseCommands.Get("Replicated/1"));
		}

		[Fact]
		public void CanSpecifyTimeoutWhenWaitingForReplication()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			SetupReplication(store1.DatabaseCommands, store2);

			using (var session = store1.OpenSession())
			{
				session.Store(new ReplicatedItem { Id = "Replicated/1" });

				session.SaveChanges();
			}

			((DocumentStore)store1).Replication.WaitAsync(timeout: TimeSpan.FromSeconds(20)).Wait();
			Assert.NotNull(store2.DatabaseCommands.Get("Replicated/1"));
		}

		[Fact]
		public async Task ShouldThrowTimeoutException()
		{
            var store1 = CreateStore(requestedStorageType: "esent");
            var store2 = CreateStore(requestedStorageType: "esent");

			SetupReplication(store1.DatabaseCommands, store2.Url + "/databases/" + store2.DefaultDatabase, "http://localhost:1234"); // the last one is not running

			using (var session = store1.OpenSession())
			{
				session.Store(new ReplicatedItem { Id = "Replicated/1" });
				session.SaveChanges();
			}

			TimeoutException timeoutException = null;

			try
			{
				await ((DocumentStore) store1).Replication.WaitAsync(timeout: TimeSpan.FromSeconds(1), replicas: 2);
			}
			catch (TimeoutException ex)
			{
				timeoutException = ex;
			}

			Assert.NotNull(timeoutException);
			Assert.Contains("was replicated to 1 of 2 servers", timeoutException.Message);
		}

		[Fact]
		public async Task ShouldThrowIfCannotReachEnoughDestinationServers()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

			SetupReplication(store1.DatabaseCommands, store2.Url + "/databases/" + store2.DefaultDatabase, "http://localhost:1234", "http://localhost:1235"); // non of them is running

			using (var session = store1.OpenSession())
			{
				session.Store(new ReplicatedItem { Id = "Replicated/1" });

				session.SaveChanges();
			}

			var exception = await AssertAsync.Throws<TimeoutException>(async () => await ((DocumentStore)store1).Replication.WaitAsync(replicas: 3));
			Assert.Contains("Confirmed that the specified etag", exception.Message);
		}

		[Fact]
		public async Task CanWaitForReplicationForOneServerEvenIfTheSecondOneIsDown()
		{
			var store1 = CreateStore();
			var store2 = CreateStore();

            SetupReplication(store1.DatabaseCommands, store2.Url + "/databases/" + store2.DefaultDatabase, "http://localhost:1234"); // the last one is not running

			using (var session = store1.OpenSession())
			{
				session.Store(new ReplicatedItem { Id = "Replicated/1" });

				session.SaveChanges();
			}

			await ((DocumentStore) store1).Replication.WaitAsync(replicas: 1);

			Assert.NotNull(store2.DatabaseCommands.Get("Replicated/1"));
		}
	}
}