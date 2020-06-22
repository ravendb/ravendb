using System.Threading.Tasks;
using FastTests.Server.Replication;
using SlowTests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15226 : ReplicationTestBase
    {
        public RavenDB_15226(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldReplicateCounters()
        {
            using (var storeA = GetDocumentStore())
            using (var storeB = GetDocumentStore())
            using (var storeC = GetDocumentStore())
            {
                var documentId = "users/1";
                using (var session = storeA.OpenAsyncSession())
                {
                    await session.StoreAsync(new User { Name = "Aviv" }, documentId);
                    await session.SaveChangesAsync();
                }

                int count = 72;

                // all these counters will go to a single CounterGroup
                for (int i = 0; i < count - 1; i++)
                {
                    using (var session = storeA.OpenAsyncSession())
                    {
                        session.CountersFor(documentId).Increment(i.ToString());
                        await session.SaveChangesAsync();
                    }
                }

                await SetupReplicationAsync(storeA, storeB);
                EnsureReplicating(storeA, storeB);

                // verify that all counters are replicated
                using (var session = storeB.OpenAsyncSession())
                {
                    var all = await session.CountersFor(documentId).GetAllAsync();
                    Assert.Equal(count - 1, all.Count);
                }

                // creating the next new counter will cause store A to split the 
                // CounterGroup document into 2 parts (with different change vectors)
                using (var session = storeA.OpenAsyncSession())
                {
                    session.CountersFor(documentId).Increment(count.ToString());
                    await session.SaveChangesAsync();
                }

                // upon incoming replication from A to B,
                // B will split the CounterGroup document into 2 parts (hopefully not with identical change vectors)
                EnsureReplicating(storeA, storeB);

                await SetupReplicationAsync(storeB, storeC);
                EnsureReplicating(storeB, storeC);

                using (var session = storeC.OpenAsyncSession())
                {
                    var doc = await session.LoadAsync<User>(documentId);
                    var metadataCounters = session.Advanced.GetCountersFor(doc);
                    Assert.Equal(count, metadataCounters.Count); 
                }

                using (var session = storeC.OpenAsyncSession())
                {
                    var all = await session.CountersFor(documentId).GetAllAsync();
                    Assert.Equal(count, all.Count);
                }
            }
        }

    }
}
