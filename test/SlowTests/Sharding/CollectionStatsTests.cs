using FastTests.Sharding;
using Raven.Client.Documents.Operations;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding
{
    public class CollectionStatsTests : ShardedTestBase
    {
        public CollectionStatsTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void GetShardedCollectionStatsTests()
        {
            using (var store = GetShardedDocumentStore())
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
                }

                var collectionStats = store.Maintenance.Send(new GetCollectionStatisticsOperation());

                Assert.Equal(3, collectionStats.Collections.Count);
                Assert.Equal(6, collectionStats.CountOfDocuments);
                Assert.Equal(0, collectionStats.CountOfConflicts);

                var detailedCollectionStats = store.Maintenance.Send(new GetDetailedCollectionStatisticsOperation());

                Assert.Equal(3, detailedCollectionStats.Collections.Count);
                Assert.Equal(6, detailedCollectionStats.CountOfDocuments);
                Assert.Equal(0, detailedCollectionStats.CountOfConflicts);
                Assert.Equal(3, detailedCollectionStats.Collections["Users"].CountOfDocuments);
                Assert.Equal(2, detailedCollectionStats.Collections["Companies"].CountOfDocuments);
                Assert.Equal(1, detailedCollectionStats.Collections["Addresses"].CountOfDocuments);

            }
        }
    }
}
