using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations.Counters;
using Raven.Client.ServerWide.Operations;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client.Counters
{
    public class CountersCluster : ClusterTestBase
    {
        public CountersCluster(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task IncrementCounterShouldNotCreateRevisions()
        {
            var (_, leader) = await CreateRaftCluster(3);
            var dbName = GetDatabaseName();
            var db = await CreateDatabaseInCluster(dbName, 3, leader.WebUrl);
            var stores = db.Servers.Select(s => new DocumentStore
            {
                Database = dbName,
                Urls = new[] { s.WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            .ToList();

            try
            {
                using (var s = stores[0].OpenSession())
                {
                    s.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    s.Store(new User { Name = "Aviv", AddressId = new string('c', 1024 * 128) }, "users/1");
                    s.SaveChanges();
                }
                var tasks = new List<Task>();

                foreach (var store in stores)
                {
                    var task = Task.Run(async () =>
                    {
                        for (var i = 0; i < 100; i++)
                        {
                            using (var s = store.OpenAsyncSession())
                            {
                                s.CountersFor("users/1").Increment($"likes/{i}", 10);
                                await s.SaveChangesAsync();
                            }
                        }
                    });

                    tasks.Add(task);
                }

                Task.WaitAll(tasks.ToArray());

                using (var session = stores[0].OpenSession())
                {
                    Assert.Equal(0, session.Advanced.Revisions.GetFor<User>("users/1").Count);
                }

            }
            finally
            {
                foreach (var item in stores)
                {
                    item.Dispose();
                }
            }
        }

        [Theory]
        [InlineData(3)]
        [InlineData(5)]
        [InlineData(7)]
        public async Task IncrementCounter(int clusterSize)
        {
            var (_, leader) = await CreateRaftCluster(clusterSize);
            var dbName = GetDatabaseName();
            var db = await CreateDatabaseInCluster(dbName, clusterSize, leader.WebUrl);
            var stores = db.Servers.Select(s => new DocumentStore
            {
                Database = dbName,
                Urls = new[] { s.WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            .ToList();

            try
            {
                using (var s = stores[0].OpenSession())
                {
                    s.Advanced.WaitForReplicationAfterSaveChanges(replicas: clusterSize - 1);
                    s.Store(new User { Name = "Aviv" }, "users/1");
                    s.SaveChanges();
                }
                var tasks = new List<Task>();
                foreach (var store in stores)
                {
                    var task = store.Operations.SendAsync(new CounterBatchOperation(new CounterBatch
                    {
                        Documents = new List<DocumentCountersOperation>
                        {
                            new DocumentCountersOperation
                            {
                                DocumentId = "users/1",
                                Operations = new List<CounterOperation>
                                {
                                    new CounterOperation
                                    {
                                        Type = CounterOperationType.Increment,
                                        CounterName = "likes",
                                        Delta = 10
                                    }
                                }
                            }
                        }
                    }));

                    tasks.Add(task);
                }

                Task.WaitAll(tasks.ToArray());

                // wait for replication and verify that all 
                // stores have the correct accumulated counter-value

                Assert.True(WaitForCounterReplication(stores, "users/1", "likes", expected: 10 * clusterSize, timeout: TimeSpan.FromSeconds(15)));

                await stores[0].Maintenance.Server.SendAsync(new DeleteDatabasesOperation(dbName, true));

            }
            finally
            {
                foreach (var item in stores)
                {
                    item.Dispose();
                }
            }
        }

        [Fact]
        public async Task DeleteCounter()
        {
            var (_, leader) = await CreateRaftCluster(3);
            var dbName = GetDatabaseName();
            var db = await CreateDatabaseInCluster(dbName, 3, leader.WebUrl);

            var stores = db.Servers.Select(s => new DocumentStore
                {
                    Database = dbName,
                    Urls = new[] { s.WebUrl },
                    Conventions = new DocumentConventions
                    {
                        DisableTopologyUpdates = true
                    }
                }.Initialize())
                .ToList();

            try
            {
                using (var s = stores[0].OpenSession())
                {
                    s.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    s.Store(new User { Name = "Aviv" }, "users/1");
                    s.Store(new User { Name = "Rotem" }, "users/2");

                    s.CountersFor("users/1").Increment("likes", 30);
                    s.CountersFor("users/2").Increment("downloads", 100);
                    s.SaveChanges();
                }

                long? val;
                foreach (var store in stores)
                {
                    val = store.Operations
                        .Send(new GetCountersOperation("users/1", new[] { "likes" }))
                        .Counters[0]?.TotalValue;
                    Assert.Equal(30, val);

                    val = store.Operations
                        .Send(new GetCountersOperation("users/2", new[] { "downloads" }))
                        .Counters[0]?.TotalValue;
                    Assert.Equal(100, val);
                }

                using (var s = stores[0].OpenSession())
                {
                    s.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    s.CountersFor("users/1").Delete("likes");
                    s.CountersFor("users/2").Delete("downloads");
                    s.SaveChanges();
                }

                foreach (var store in stores)
                {
                    var countersDetail = store.Operations.Send(new GetCountersOperation("users/1", new[] { "likes" }));
                    Assert.Equal(1, countersDetail.Counters.Count);
                    Assert.Null(countersDetail.Counters[0]);

                    countersDetail = store.Operations.Send(new GetCountersOperation("users/2", new[] { "downloads" }));
                    Assert.Equal(1, countersDetail.Counters.Count);
                    Assert.Null(countersDetail.Counters[0]);
                }
            }
            finally
            {
                foreach (var item in stores)
                {
                    item.Dispose();
                }
            }
        }

    }
}
