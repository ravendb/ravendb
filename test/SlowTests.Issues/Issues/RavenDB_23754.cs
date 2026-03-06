using System;
using System.Collections.Generic;
using System.IO;
using System.Text;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.ServerWide;
using Raven.Client.ServerWide.Operations;
using Raven.Server;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_23754 : ReplicationTestBase
    {
        public RavenDB_23754(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task GetDetailedCollectionStats_ShouldReturnCorrectConflictCounts()
        {
            var (nodes, leader) = await CreateRaftCluster(2, watcherCluster: true);
            using var store = GetDocumentStore(new Options()
            {
                ReplicationFactor = 2,
                Server = leader
            });

            var usersCollection = store.Conventions.FindCollectionName(typeof(User));
            var scriptResolver = new ScriptResolver
            {
                Script = "return doc[10];"
            };
            var op = new ModifyConflictSolverOperation(
                database: store.Database,
                collectionByScript: new Dictionary<string, ScriptResolver>
                {
                    [usersCollection] = scriptResolver
                },
                resolveToLatest: false
            );
            store.Maintenance.Server.Send(op);

            using var store1 = GetStoreForServer(nodes[0], store.Database);
            using var store2 = GetStoreForServer(nodes[1], store.Database);

            var r1 = BreakReplication(nodes[0].ServerStore, store.Database);
            var r2 = BreakReplication(nodes[1].ServerStore, store.Database);

            using (var session = store1.OpenAsyncSession())
            {
                await session.StoreAsync(new User()
                {
                    Name = "Golan1"
                }, UsersId);
                await session.SaveChangesAsync();
            }

            using (var session = store2.OpenAsyncSession())
            {
                await session.StoreAsync(new User()
                {
                    Name = "Golan2"
                }, UsersId);
                await session.SaveChangesAsync();
            }

            r1.Result.Mend();
            r2.Result.Mend();

            await WaitAndAssertForValueAsync(() =>
                {
                    var detailedCollectionStats = store1.Maintenance.Send(new GetDetailedCollectionStatisticsOperation());
                    return (detailedCollectionStats.CountOfConflicts, detailedCollectionStats.CountOfDocumentsConflicts);
                }, 
                expectedVal: (2, 1),
                timeout: 5000,
                interval: 500);
        }
        private IDocumentStore GetStoreForServer(RavenServer server, string database)
        {
            return new DocumentStore
            {
                Database = database,
                Urls = new[] { server.WebUrl },
                Conventions = new DocumentConventions { DisableTopologyUpdates = true }
            }.Initialize();
        }

        private const string UsersId = "Users/1";

        [RavenFact(RavenTestCategory.ClientApi)]
        public void GetCollectionStatsTests()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User() { Name = "user1" }, "users/1");
                    session.Store(new User() { Name = "user2" }, "users/2");
                    session.Store(new User() { Name = "user3" }, "users/3");
                    session.Store(new Company() { Name = "com1" }, "com/1");
                    session.Store(new Company() { Name = "com2" }, "com/2");
                    session.Store(new Address() { City = "city1" }, "add/1");

                    session.SaveChanges();

                    session.Advanced.Revisions.ForceRevisionCreationFor("users/1");
                    session.Advanced.Revisions.ForceRevisionCreationFor("com/1");

                    session.Delete("users/3");

                    session.Advanced.Attachments.Store(
                        "users/1", "hello.txt",
                        new MemoryStream(Encoding.UTF8.GetBytes("hello")));

                    session.CountersFor("users/1").Increment("likes", 5);
                    session.CountersFor("com/1").Increment("views", 10);

                    var tsTime = DateTime.UtcNow;
                    var ts = session.TimeSeriesFor("users/1", "heartrate");
                    ts.Append(tsTime, 72, "wrist");

                    ts.Delete(tsTime.AddMinutes(-5), tsTime.AddMinutes(-1));

                    session.SaveChanges();
                }

                var collectionStats = store.Maintenance.Send(new GetCollectionStatisticsOperation());

                Assert.Equal(3, collectionStats.Collections.Count);
                Assert.Equal(5, collectionStats.CountOfDocuments);
                Assert.Equal(0, collectionStats.CountOfConflicts);

                var detailedCollectionStats = store.Maintenance.Send(new GetDetailedCollectionStatisticsOperation());

                Assert.Equal(3, detailedCollectionStats.Collections.Count);
                Assert.Equal(5, detailedCollectionStats.CountOfDocuments);
                Assert.Equal(0, detailedCollectionStats.CountOfConflicts);
                Assert.Equal(2, detailedCollectionStats.CountOfRevisionDocuments);
                Assert.Equal(1, detailedCollectionStats.CountOfTombstones);
                Assert.Equal(1, detailedCollectionStats.CountOfAttachments);
                Assert.Equal(2, detailedCollectionStats.CountOfCounterEntries);
                Assert.Equal(1, detailedCollectionStats.CountOfTimeSeriesSegments);
                Assert.Equal(1, detailedCollectionStats.CountOfTimeSeriesDeletedRanges);
                Assert.Equal(0, detailedCollectionStats.CountOfDocumentsConflicts);
                Assert.Equal(2, detailedCollectionStats.Collections["Users"].CountOfDocuments);
                Assert.Equal(2, detailedCollectionStats.Collections["Companies"].CountOfDocuments);
                Assert.Equal(1, detailedCollectionStats.Collections["Addresses"].CountOfDocuments);
            }
        }
    }
}


