using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Client.Counters
{
    public class CountersCrudMultipuleNodes : ClusterTestBase
    {
        [Fact]
        public async Task IncrementCounter()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
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
                    s.SaveChanges();
                }
                var tasks = new List<Task>();
                foreach (var store in stores)
                {
                    var task = store.Counters.IncrementAsync("users/1", "likes", 10);
                    tasks.Add(task);
                }

                Task.WaitAll(tasks.ToArray());

                foreach (var store in stores)
                {
                    long? val = null;
                    for (int i = 0; i < 100; i++)
                    {
                        val = store.Counters.Get("users/1", "likes");
                        if (val == 30)
                            break;
                        Thread.Sleep(50);
                    }

                    Assert.Equal(30, val);
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

        [Fact]
        public async Task DeleteCounter()
        {
            var leader = await CreateRaftClusterAndGetLeader(3);
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
                    s.Advanced.Counters.Increment("users/1", "likes", 30);
                    s.SaveChanges();
                }

                long? val;
                foreach (var store in stores)
                {
                    val = store.Counters.Get("users/1", "likes");
                    Assert.Equal(30, val);
                }

                using (var s = stores[0].OpenSession())
                {
                    s.Advanced.WaitForReplicationAfterSaveChanges(replicas: 2);
                    s.Advanced.Counters.Delete("users/1", "likes");
                    s.SaveChanges();
                }

                foreach (var store in stores)
                {
                    val = store.Counters.Get("users/1", "likes");
                    Assert.Null(val);
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
