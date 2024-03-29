﻿using System.Threading.Tasks;
using SlowTests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15226 : ReplicationTestBase
    {
        public RavenDB_15226(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Counters | RavenTestCategory.Replication)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ShouldReplicateCounters(Options options)
        {
            using (var storeA = GetDocumentStore(options))
            using (var storeB = GetDocumentStore(options))
            using (var storeC = GetDocumentStore(options))
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
                await EnsureReplicatingAsync(storeA, storeB);

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
                await EnsureReplicatingAsync(storeA, storeB);

                await SetupReplicationAsync(storeB, storeC);
                await EnsureReplicatingAsync(storeB, storeC);

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
