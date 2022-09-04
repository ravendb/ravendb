using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Parquet.Thrift;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Raven.Client.Json;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17157 : ClusterTestBase
    {
        public RavenDB_17157(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void OnSessionCreated_WaitForIndexesAfterSaveChanges_DisableIndexTest()
        {
            using (var store = GetDocumentStore())
            {
                var userIndex = new UsersIndex();
                string indexName = userIndex.IndexName;
                userIndex.Execute(store);
                store.Maintenance.Send(new DisableIndexOperation(indexName));
                string[] indexes = { indexName };

                store.OnSessionCreated += (sender, sessionCreatedEventArgs) =>
                {
                    sessionCreatedEventArgs.Session.WaitForIndexesAfterSaveChanges(timeout: TimeSpan.FromMilliseconds(500), throwOnTimeout: true, indexes: indexes);
                };

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Foo" });
                    session.Store(new User { Name = "Bar" });
                    var exception = Assert.Throws<RavenTimeoutException>(() => session.SaveChanges());

                    Assert.Contains("total stale indexes: 1", exception.Message);
                }
            }
        }

        [Fact]
        public void OnSessionCreated_WaitForIndexesAfterSaveChanges_Test()
        {
            using (var store = GetDocumentStore())
            {
                store.OnSessionCreated += (sender, sessionCreatedEventArgs) =>
                {
                    sessionCreatedEventArgs.Session.WaitForIndexesAfterSaveChanges();
                };
                
                new UsersIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Foo" });
                    session.Store(new User { Name = "Bar" });
                    session.SaveChanges();

                    QueryStatistics stats;
                    var results = ExecuteQuery();

                    Assert.Equal(1, results.Count);
                    Assert.False(stats.IsStale);

                    session.Store(new User { Name = "FooBar" });
                    session.SaveChanges();

                    results = ExecuteQuery();

                    Assert.Equal(2, results.Count);
                    Assert.False(stats.IsStale);

                    List<User> ExecuteQuery()
                    {
                        return session.Query<UsersIndex.Result, UsersIndex>()
                            .Statistics(out stats)
                            .Where(x => x.MyName.StartsWith("Foo"))
                            .As<User>()
                            .ToList();
                    }
                }
            }
        }

        [Fact]
        public async Task OnSessionCreated_WaitForReplicationAfterSaveChanges_Test()
        {
            var db = "DatabaseNodes";
            var (nodes, leader) = await CreateRaftCluster(3);
            await CreateDatabaseInCluster(db, 3, leader.WebUrl);

            using (var store1 = new DocumentStore
                   {
                       Database = db,
                       Urls = new[] { nodes[0].WebUrl },
                       Conventions = new Raven.Client.Documents.Conventions.DocumentConventions
                       {
                           DisableTopologyUpdates = true
                       }
            }.Initialize())
            using (var store2 = new DocumentStore
            {
                Database = db,
                Urls = new[] { nodes[1].WebUrl },
                Conventions = new Raven.Client.Documents.Conventions.DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())
            using (var store3 = new DocumentStore
            {
                Database = db,
                Urls = new[] { nodes[2].WebUrl },
                Conventions = new Raven.Client.Documents.Conventions.DocumentConventions
                {
                    DisableTopologyUpdates = true
                }
            }.Initialize())

            {
                store1.OnSessionCreated += (sender, sessionCreatedEventArgs) =>
                {
                    sessionCreatedEventArgs.Session.WaitForReplicationAfterSaveChanges(replicas: 2);
                };

                using (var session = store1.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new User { Name = "Foo" }, i.ToString());
                    }

                    session.SaveChanges();
                }

                using (var session = store2.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("4"));
                    Assert.NotNull(session.Load<User>("1"));
                }

                using (var session = store3.OpenSession())
                {
                    Assert.NotNull(session.Load<User>("4"));
                    Assert.NotNull(session.Load<User>("1"));
                }
            }
        }

        private class UsersIndex : AbstractIndexCreationTask<User>
        {
            public class Result
            {
                public string MyName { get; set; }
            }

            public UsersIndex()
            {
                Map = users => 
                    from user in users
                    select new Result
                    {
                        MyName = user.Name
                    };
            }
        }
    }
}

