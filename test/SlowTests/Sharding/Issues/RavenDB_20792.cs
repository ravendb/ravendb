using System.Threading.Tasks;
using FastTests;
using Raven.Server.ServerWide.Context;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Sharding.Issues;

public class RavenDB_20792 : RavenTestBase
{
    public RavenDB_20792(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task OptimisticConcurrency_ShouldOnlyCompareChangeVectorVersions_OnPut()
    {
        using var store = Sharding.GetDocumentStore();

        const string id = "users/1";
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User
            {
                Name = "aviv"
            }, id);
            await session.SaveChangesAsync();
        }

        string changeVector;
        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<User>(id);
            changeVector = session.Advanced.GetChangeVectorFor(doc);
        }

        await Sharding.Resharding.MoveShardForId(store, id);
        await Sharding.Resharding.MoveShardForId(store, id);

        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<User>(id);
            var cvAfterResharding = session.Advanced.GetChangeVectorFor(doc);

            Assert.NotEqual(cvAfterResharding, changeVector);

            using (Server.ServerStore.ContextPool.AllocateOperationContext(out TransactionOperationContext context))
            {
                var versionPart = context.GetChangeVector(cvAfterResharding).Version;
                Assert.Equal(versionPart, changeVector);
            }
        }

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User
            {
                Name = "ayende"
            }, changeVector: changeVector, id);

            // should not throw concurrency exception
            await session.SaveChangesAsync();
        }
    }

    [Fact]
    public async Task OptimisticConcurrency_ShouldOnlyCompareChangeVectorVersions_OnDelete()
    {
        using var store = Sharding.GetDocumentStore();

        const string id = "users/1";
        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new User(), id);
            await session.SaveChangesAsync();
        }

        string changeVector;
        using (var session = store.OpenAsyncSession())
        {
            var doc = await session.LoadAsync<User>(id);
            changeVector = session.Advanced.GetChangeVectorFor(doc);
        }

        await Sharding.Resharding.MoveShardForId(store, id);
        await Sharding.Resharding.MoveShardForId(store, id);

        using (var session = store.OpenAsyncSession())
        {
            session.Delete(id, expectedChangeVector: changeVector);

            // should not throw concurrency exception
            await session.SaveChangesAsync();
        }
    }
}
