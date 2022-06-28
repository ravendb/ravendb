using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Exceptions;
using Raven.Server;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15497 : ClusterTestBase
    {
        public RavenDB_15497(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task WaitForIndexesAfterSaveChangesCanExitWhenThrowOnTimeoutIsFalse()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Index();
                await index.ExecuteAsync(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "user1",
                        Count = 3
                    });
                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(3), throwOnTimeout: false);
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                await store.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "user1"
                    });
                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(3), throwOnTimeout: true);

                    var error = Assert.Throws<RavenTimeoutException>(() => session.SaveChanges());
                    Assert.StartsWith("Raven.Client.Exceptions.RavenTimeoutException", error.Message);
                    Assert.Contains("could not verify that all indexes has caught up with the changes as of etag", error.Message);
                    Assert.Contains("total paused indexes: 1", error.Message);
                    Assert.DoesNotContain("total errored indexes", error.Message);
                }
            }
        }

        [Fact]
        public async Task WaitForIndexesAfterSaveChangesCanExitWhenThrowOnTimeoutIsFalse_Cluster()
        {
            var databaseName = GetDatabaseName();
            (List<RavenServer> _, RavenServer leader) = await CreateRaftCluster(3);
            var (raftIndex, dbGroupNodes) = await CreateDatabaseInCluster(databaseName, 3, leader.WebUrl);
            await Cluster.WaitForRaftIndexToBeAppliedInClusterAsync(raftIndex, TimeSpan.FromSeconds(30));

            using (var store = new DocumentStore
            {
                Database = databaseName,
                Urls = new[] { dbGroupNodes[0].WebUrl }
            }.Initialize())
            using (var store1 = new DocumentStore
            {
                Database = databaseName,
                Urls = new[] { dbGroupNodes[0].WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            using (var store2 = new DocumentStore
            {
                Database = databaseName,
                Urls = new[] { dbGroupNodes[1].WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            using (var store3 = new DocumentStore
            {
                Database = databaseName,
                Urls = new[] { dbGroupNodes[2].WebUrl },
                Conventions = new DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            {
                var index = new Index();

                var results = await store.Maintenance.SendAsync(new PutIndexesOperation(index.CreateIndexDefinition()));
                var result = results[0];

                await Cluster.WaitForRaftIndexToBeAppliedOnClusterNodesAsync(result.RaftCommandIndex, dbGroupNodes, TimeSpan.FromSeconds(15));

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "user1",
                        Count = 3
                    });
                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), throwOnTimeout: false);
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store1);
                Indexes.WaitForIndexing(store2);
                Indexes.WaitForIndexing(store3);

                await store1.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));
                await store2.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));
                await store3.Maintenance.SendAsync(new StopIndexOperation(index.IndexName));

                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "user1"
                    });
                    session.Advanced.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromSeconds(10), throwOnTimeout: true);

                    var error = Assert.Throws<RavenTimeoutException>(() => session.SaveChanges());
                    Assert.StartsWith("Raven.Client.Exceptions.RavenTimeoutException", error.Message);
                    Assert.Contains("could not verify that all indexes has caught up with the changes as of etag", error.Message);
                    Assert.Contains("total paused indexes: 1", error.Message);
                    Assert.DoesNotContain("total errored indexes", error.Message);
                }
            }
        }

        private class Index : AbstractIndexCreationTask<User>
        {
            public Index()
            {
                Map = users =>
                    from user in users
                    select new
                    {
                        user.Name
                    };
            }
        }
    }
}
