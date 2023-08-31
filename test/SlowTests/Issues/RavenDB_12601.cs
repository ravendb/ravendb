using System.Threading.Tasks;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Session;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12601 : ReplicationTestBase
    {
        public RavenDB_12601(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Change_vector_of_cluster_tx_updated_correctly()
        {
            using (var store = GetDocumentStore())
            {
                const string userId = "users/1";

                using (var session = store.OpenSession())
                {
                    var user = new User
                    {
                        Id = userId,
                        Name = "User1"
                    };

                    session.Store(user);
                    session.SaveChanges();

                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    Assert.True(changeVector.Contains("A:1"));
                }

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                var databaseChangeVector = stats.DatabaseChangeVector;
                Assert.True(databaseChangeVector.Contains("A:1"));

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var user = session.Load<User>(userId);
                    user.Age++;
                    session.SaveChanges();

                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    Assert.True(changeVector.Contains("RAFT:1"));
                }

                stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                databaseChangeVector = stats.DatabaseChangeVector;
                Assert.True(databaseChangeVector.Contains("A:1") && databaseChangeVector.Contains("RAFT:1"));

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var user = session.Load<User>(userId);
                    user.Age++;
                    session.SaveChanges();

                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    Assert.True(changeVector.Contains("RAFT:2"));
                }

                stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                databaseChangeVector = stats.DatabaseChangeVector;
                Assert.True(databaseChangeVector.Contains("A:1") && databaseChangeVector.Contains("RAFT:2"));
            }
        }

        [RavenTheory(RavenTestCategory.ClusterTransactions)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Single)]
        public async Task Change_vector_of_cluster_tx_updated_correctly_in_cluster(Options options)
        {
            var (source, destination) = await CreateDuoCluster(options);

            using (source)
            using (destination)
            {
                const string userId = "users/1";
                using (var session = source.OpenSession())
                {
                    session.Store(new User
                    {
                        Id = userId,
                        Name = "User1"
                    });
                    session.SaveChanges();
                }

                WaitForDocumentToReplicate<User>(source, userId, 15_000);
                WaitForDocumentToReplicate<User>(destination, userId, 15_000);

                // we want that the change vector of the document will contain both nodes, A & B
                var result = await WaitForValueAsync(() =>
                {
                    using (var session = source.OpenSession())
                    {
                        var user = session.Load<User>(userId);
                        user.Age++;
                        session.SaveChanges();

                        var changeVector = session.Advanced.GetChangeVectorFor(user);
                        return changeVector.Contains("A:") && changeVector.Contains("B:");
                    }
                }, true, 15_000);

                Assert.True(result);

                WaitForDocumentToReplicate<User>(source, userId, 15_000);
                WaitForDocumentToReplicate<User>(destination, userId, 15_000);

                using (var session = source.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var user = session.Load<User>(userId);
                    user.Age++;
                    session.SaveChanges();

                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    Assert.True(changeVector.Contains("RAFT:1"), $"changeVector is: {changeVector}");
                }

                var stats = await source.Maintenance.SendAsync(new GetStatisticsOperation());
                var databaseChangeVector = stats.DatabaseChangeVector;
                Assert.True(databaseChangeVector.Contains("A:") &&
                            databaseChangeVector.Contains("B:") &&
                            databaseChangeVector.Contains("RAFT:1"),
                    $"changeVector is: {databaseChangeVector}");
            }
        }

        [Fact]
        public async Task Can_use_cluster_tx_mixed_with_tx()
        {
            DoNotReuseServer();
            long trxn1 = -1;
            using (var store = GetDocumentStore())
            {
                const string userId = "users/1";

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var user = new User
                    {
                        Id = userId,
                        Name = "User1"
                    };

                    session.Store(user);
                    session.SaveChanges();

                    trxn1 = Cluster.LastRaftIndexForCommand(Server, "ClusterTransactionCommand");
                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    Assert.True(changeVector.Contains("RAFT:1"), $"{changeVector}.Contains('RAFT:1')");
                    Assert.True(changeVector.Contains($"TRXN:{trxn1}"), $"{changeVector}.Contains('TRXN:{trxn1}')");
                }

                var stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                var databaseChangeVector = stats.DatabaseChangeVector;
                Assert.True(databaseChangeVector.Contains("RAFT:1"), $"{databaseChangeVector}.Contains('RAFT:1')");
                Assert.False(databaseChangeVector.Contains("TRXN"), $"{databaseChangeVector}.Contains('TRXN')");

                using (var session = store.OpenSession())
                {
                    var user = session.Load<User>(userId);
                    user.Age++;
                    session.SaveChanges();

                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    Assert.True(changeVector.Contains("RAFT:1"), $"{changeVector}.Contains('RAFT:1')");
                    Assert.True(changeVector.Contains("A:2"), $"{changeVector}.Contains('A:2')");
                }

                stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                databaseChangeVector = stats.DatabaseChangeVector;
                Assert.True(databaseChangeVector.Contains("A:2"), $"{databaseChangeVector}.Contains('A:2')");
                Assert.True(databaseChangeVector.Contains("RAFT:1"), $"{databaseChangeVector}.Contains('RAFT:1')");
                Assert.False(databaseChangeVector.Contains("TRXN"), $"{databaseChangeVector}.Contains('TRXN')");

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var user = session.Load<User>(userId);
                    user.Age++;
                    session.SaveChanges();
                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    var trxn2 = Cluster.LastRaftIndexForCommand(Server, "ClusterTransactionCommand");
                    Assert.True(trxn2 > trxn1);
                    Assert.True(changeVector.Contains("RAFT:2"), $"{changeVector}.Contains('RAFT:2')");
                    Assert.True(changeVector.Contains($"TRXN:{trxn2}"), $"{changeVector}.Contains('TRXN:{trxn2}')");

                }

                stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                databaseChangeVector = stats.DatabaseChangeVector;
                Assert.True(databaseChangeVector.Contains("A:2"), $"{databaseChangeVector}.Contains('A:2')");
                Assert.True(databaseChangeVector.Contains("RAFT:2"), $"{databaseChangeVector}.Contains('RAFT:2')");
                Assert.False(databaseChangeVector.Contains("TRXN"), $"{databaseChangeVector}.Contains('TRXN')");


                using (var session = store.OpenSession())
                {

                    session.Store(new User(), "new-user");
                    session.SaveChanges();

                    var user = session.Load<User>("new-user");
                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    Assert.True(changeVector.Contains("RAFT:2"), $"{changeVector}.Contains('RAFT:2')");
                    Assert.True(changeVector.Contains("A:5"), $"{changeVector}.Contains('A:5')");
                    Assert.False(changeVector.Contains("TRXN"), $"{changeVector}.Contains('TRXN')");

                    stats = await store.Maintenance.SendAsync(new GetStatisticsOperation());
                    databaseChangeVector = stats.DatabaseChangeVector;
                    Assert.True(databaseChangeVector.Contains("A:5"), $"{databaseChangeVector}.Contains('A:5')");
                    Assert.True(databaseChangeVector.Contains("RAFT:2"), $"{databaseChangeVector}.Contains('RAFT:2')");
                    Assert.False(databaseChangeVector.Contains("TRXN"), $"{databaseChangeVector}.Contains('TRXN')");
                }

                WaitForUserToContinueTheTest(store);
            }
        }

        private class User
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public int Age { get; set; }
        }
    }
}
