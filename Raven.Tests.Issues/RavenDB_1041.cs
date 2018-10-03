// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1041.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Abstractions.Connection;
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
            
          
            await store1.Replication.WaitAsync(replicas:2);

            Assert.NotNull(store2.DatabaseCommands.ForDatabase(DatabaseName).Get("Replicated/1"));
            Assert.NotNull(store3.DatabaseCommands.ForDatabase(DatabaseName).Get("Replicated/1"));

        }

        [Fact]
        public async Task CanWaitForReplicationOfParticularEtag()
        {

            var store1 = CreateStore(requestedStorageType: "esent", databaseName: "CanWaitForReplicationOfParticularEtag_Store1");
            var store2 = CreateStore(requestedStorageType: "esent", databaseName: "CanWaitForReplicationOfParticularEtag_Store2");
            var store3 = CreateStore(requestedStorageType: "esent", databaseName: "CanWaitForReplicationOfParticularEtag_Store3");

            SetupReplication(store1.DatabaseCommands, store2, store3);

            var putResult = store1.DatabaseCommands.Put("Replicated/1", null, new RavenJObject(), new RavenJObject());
            var putResult2 = store1.DatabaseCommands.Put("Replicated/2", null, new RavenJObject(), new RavenJObject());

            await store1.Replication.WaitAsync(putResult.ETag);

            Assert.NotNull(store2.DatabaseCommands.Get("Replicated/1"));
            Assert.NotNull(store3.DatabaseCommands.Get("Replicated/1"));

            await store1.Replication.WaitAsync(putResult2.ETag);


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

            await store1.Replication.WaitAsync(timeout: TimeSpan.FromSeconds(10),replicas:2);

            Assert.NotNull(store2.DatabaseCommands.Get("Replicated/1"));
            Assert.NotNull(store3.DatabaseCommands.Get("Replicated/1"));
        }

        [Fact]
        public void CanSpecifyTimeoutWhenWaitingForReplication()
        {
            ShowLogs = true;
            var store1 = CreateStore();
            var store2 = CreateStore();

            SetupReplication(store1.DatabaseCommands, store2);

            using (var session = store1.OpenSession())
            {
                session.Store(new ReplicatedItem { Id = "Replicated/1" });
                session.SaveChanges();
            }

            store1.Replication.WaitAsync(timeout: TimeSpan.FromSeconds(20)).Wait();
            Assert.NotNull(store2.DatabaseCommands.Get("Replicated/1"));
        }

        [Theory(Skip = "Flaky test")]
        [PropertyData("Storages")]
        public async Task ShouldThrowTimeoutException(string storageName)
        {
            using (var store1 = CreateStore(requestedStorageType: storageName))
            using (var store2 = CreateStore(requestedStorageType: storageName))
            {
                SetupReplication(store1.DatabaseCommands, store2.Url + "/databases/" + store2.DefaultDatabase, "http://localhost:1234"); // the last one is not running

                using (var session = store1.OpenSession())
                {
                    session.Store(new ReplicatedItem {Id = "Replicated/1"});
                    session.SaveChanges();
                }

                TimeoutException timeoutException = null;

                try
                {
                    //This test is racy, it depends on the time it takes to fetch the topology
                    await ((DocumentStore)store1).Replication.WaitAsync(timeout: TimeSpan.FromSeconds(10), replicas: 2);
                }
                catch (TimeoutException ex)
                {
                    timeoutException = ex;
                }

                Assert.NotNull(timeoutException);
                Assert.Contains("So far, it only replicated to 1", timeoutException.Message);
            }
        }

        [Fact(Skip = "Flaky")]
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
            Assert.Contains("Could not verify that etag", exception.Message);
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

            await ((DocumentStore)store1).Replication.WaitAsync(replicas: 1);

            Assert.NotNull(store2.DatabaseCommands.Get("Replicated/1"));
        }
    }
}
