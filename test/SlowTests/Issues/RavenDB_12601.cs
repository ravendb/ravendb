using System.Threading.Tasks;
using FastTests.Server.Replication;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12601 : ReplicationTestBase
    {
        [Fact]
        public void Change_vector_of_cluster_tx_updated_correctly()
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

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var user = session.Load<User>(userId);
                    user.Age++;
                    session.SaveChanges();

                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    Assert.True(changeVector.Contains("A:1") && changeVector.Contains("RAFT:1"));
                }

                using (var session = store.OpenSession(new SessionOptions
                {
                    TransactionMode = TransactionMode.ClusterWide
                }))
                {
                    var user = session.Load<User>(userId);
                    user.Age++;
                    session.SaveChanges();

                    var changeVector = session.Advanced.GetChangeVectorFor(user);
                    Assert.True(changeVector.Contains("A:1") && changeVector.Contains("RAFT:2"));
                }
            }
        }

        [Fact]
        public async Task Change_vector_of_cluster_tx_updated_correctly_in_cluster()
        {
            var (source, destination) = await CreateDuoCluster();

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
                while (true)
                {
                    using (var session = source.OpenSession())
                    {
                        var user = session.Load<User>(userId);
                        user.Age++;
                        session.SaveChanges();

                        var changeVector = session.Advanced.GetChangeVectorFor(user);
                        if (changeVector.Contains("A:") && changeVector.Contains("B:"))
                            break;

                        await Task.Delay(10);
                    }
                }

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
                    Assert.True(changeVector.Contains("A:") && changeVector.Contains("B:") && changeVector.Contains("RAFT:1"), 
                        $"changeVector is: {changeVector}");
                }
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
