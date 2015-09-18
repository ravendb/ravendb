// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1041.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Threading.Tasks;

using Raven.Abstractions.Util;
using Raven.Client.Document;
using Raven.Database.Extensions;
using Raven.Json.Linq;
using Raven.Tests.Common;

using Xunit;
using Xunit.Extensions;

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

        [Theory]
        [PropertyData("Storages")]
		public async Task CanWaitForReplication(string storage)
		{
			using (var store1 = CreateStore(requestedStorage: storage, databaseName: DatabaseName))
			using (var store2 = CreateStore(requestedStorage: storage, databaseName: DatabaseName))
            using (var store3 = CreateStore(requestedStorage: storage, databaseName: DatabaseName))
            {
                SetupReplication(store1.DatabaseCommands, store2, store3);

                using (var session = store1.OpenSession(DatabaseName))
                {
                    session.Store(new ReplicatedItem { Id = "Replicated/1" });

                    session.SaveChanges();
                }

                var i = await ((DocumentStore)store1).Replication.WaitAsync(database: DatabaseName);
                Assert.Equal(2, i);


                Assert.NotNull(store2.DatabaseCommands.ForDatabase(DatabaseName).Get("Replicated/1"));
                Assert.NotNull(store3.DatabaseCommands.ForDatabase(DatabaseName).Get("Replicated/1"));
            }
		}

        [Theory]
        [PropertyData("Storages")]
        public async Task CanWaitForReplicationOfParticularEtag(string storage)
		{
            using (var store1 = CreateStore(requestedStorage: storage, databaseName: "CanWaitForReplicationOfParticularEtag_Store1"))
            using (var store2 = CreateStore(requestedStorage: storage, databaseName: "CanWaitForReplicationOfParticularEtag_Store2"))
            using (var store3 = CreateStore(requestedStorage: storage, databaseName: "CanWaitForReplicationOfParticularEtag_Store3"))
            {
                SetupReplication(store1.DatabaseCommands, store2, store3);

                var putResult = store1.DatabaseCommands.Put("Replicated/1", null, new RavenJObject(), new RavenJObject());
                var putResult2 = store1.DatabaseCommands.Put("Replicated/2", null, new RavenJObject(), new RavenJObject());

                var i = await ((DocumentStore)store1).Replication.WaitAsync(putResult.ETag);
                Assert.Equal(2, i);

                Assert.NotNull(store2.DatabaseCommands.Get("Replicated/1"));
                Assert.NotNull(store3.DatabaseCommands.Get("Replicated/1"));

                i = await ((DocumentStore)store1).Replication.WaitAsync(putResult2.ETag);
                Assert.Equal(2, i);

                Assert.NotNull(store2.DatabaseCommands.Get("Replicated/2"));
                Assert.NotNull(store3.DatabaseCommands.Get("Replicated/2"));
            }
		}

        [Theory]
        [PropertyData("Storages")]
        public async Task CanWaitForReplicationInAsyncManner(string storage)
		{
            using (var store1 = CreateStore(requestedStorage: storage))
            using (var store2 = CreateStore(requestedStorage: storage))
            using (var store3 = CreateStore(requestedStorage: storage))
            {                
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
		}

        [Theory]
        [PropertyData("Storages")]
        public void CanSpecifyTimeoutWhenWaitingForReplication(string storage)
		{
            using (var store1 = CreateStore(requestedStorage: storage))
            using (var store2 = CreateStore(requestedStorage: storage))
            {
                ShowLogs = true;

                SetupReplication(store1.DatabaseCommands, store2);

                using (var session = store1.OpenSession())
                {
                    session.Store(new ReplicatedItem { Id = "Replicated/1" });
                    session.SaveChanges();
                }

                store1.Replication.WaitAsync(timeout: TimeSpan.FromSeconds(20)).Wait();
                Assert.NotNull(store2.DatabaseCommands.Get("Replicated/1"));
            }
		}

        [Theory]
        [PropertyData("Storages")]
        public void ShouldThrowTimeoutException(string storage)
		{
            var store1 = CreateStore(requestedStorage: storage);
			var store2 = CreateStore(requestedStorage: storage);

			SetupReplication(store1.DatabaseCommands, store2.Url + "/databases/" + store2.DefaultDatabase, "http://localhost:1234"); // the last one is not running

			using (var session = store1.OpenSession())
			{
				session.Store(new ReplicatedItem { Id = "Replicated/1" });
				session.SaveChanges();
			}

		    Assert.Throws<TimeoutException>(() => 
			    // ReSharper disable once RedundantArgumentDefaultValue
				AsyncHelpers.RunSync(() => store1.Replication.WaitAsync(timeout: TimeSpan.FromSeconds(1), replicas: 2)));
		}

        [Theory]
        [PropertyData("Storages")]
        public async Task ShouldThrowIfCannotReachEnoughDestinationServers(string storage)
		{
            using (var store1 = CreateStore(requestedStorage: storage))
            using (var store2 = CreateStore(requestedStorage: storage))
            {
                SetupReplication(store1.DatabaseCommands, store2.Url + "/databases/" + store2.DefaultDatabase, "http://localhost:1234", "http://localhost:1235"); // non of them is running

                using (var session = store1.OpenSession())
                {
                    session.Store(new ReplicatedItem { Id = "Replicated/1" });

                    session.SaveChanges();
                }

                var exception = await AssertAsync.Throws<TimeoutException>(async () => await ((DocumentStore)store1).Replication.WaitAsync(replicas: 3));
                Assert.Contains("Could only confirm that the specified Etag", exception.Message);
            }
	    }


        [Theory]
        [PropertyData("Storages")]
        public async Task CanWaitForReplicationForOneServerEvenIfTheSecondOneIsDown(string storage)
		{
            using (var store1 = CreateStore(requestedStorage: storage))
            using (var store2 = CreateStore(requestedStorage: storage))
            {
                SetupReplication(store1.DatabaseCommands, store2.Url + "/databases/" + store2.DefaultDatabase, "http://localhost:1234"); // the last one is not running

                using (var session = store1.OpenSession())
                {
                    session.Store(new ReplicatedItem { Id = "Replicated/1" });

                    session.SaveChanges();
                }

                await ((DocumentStore)store1).Replication.WaitAsync(replicas: 1);

                Assert.NotNull(store2.DatabaseCommands.Get("Replicated/1"));
            }
		}
	}
}