using System.Threading.Tasks;
using FastTests.Server.Replication;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_19421_2 : ReplicationTestBase
{
    public RavenDB_19421_2(ITestOutputHelper output) : base(output)
    {
    }

    [Fact]
    public async Task ConflictsOnIdenticalDocument()
    {
        using var store1 = GetDocumentStore();
        using var store2 = GetDocumentStore();
        using var store3 = GetDocumentStore();

        using (var s1 = store1.OpenAsyncSession())
        {
            await s1.StoreAsync(new { Val = 1 }, "users/1");
            await s1.SaveChangesAsync();
        }

        await SetupReplicationAsync(store1, store3);
        await EnsureReplicatingAsync(store1, store3);
          
        using (var s3 = store3.OpenAsyncSession())
        {
            await s3.StoreAsync(new { Val = 3 }, "users/1");
            await s3.SaveChangesAsync();
        }

        // will be replicated to 3 and resolved to latest
        using (var s1 = store1.OpenAsyncSession())
        {
            await s1.StoreAsync(new { Val = 4 }, "users/1");
            await s1.SaveChangesAsync();
        }
        await EnsureReplicatingAsync(store1, store3);

        using (var s3 = store3.OpenAsyncSession())
        {
            await s3.StoreAsync(new { Val = 5 }, "users/1");
            await s3.StoreAsync(new { Val = 4 }, "users/4");
            await s3.SaveChangesAsync();
        }
        
        // here we resolve identical value, merging the change vectors
        using (var s1 = store1.OpenAsyncSession())
        {
            await s1.StoreAsync(new { Val = 5 }, "users/1");
            await s1.SaveChangesAsync();
        }
        await EnsureReplicatingAsync(store1, store3);
        
        await SetupReplicationAsync(store3, store1);
        await EnsureReplicatingAsync(store3, store1);
        
        await SetupReplicationAsync(store3, store2);
        await SetupReplicationAsync(store1, store2);

        await EnsureReplicatingAsync(store3, store2);
        await EnsureReplicatingAsync(store1, store2);

        using (var s1 = store1.OpenAsyncSession())
        using (var s2 = store2.OpenAsyncSession())
        using (var s3 = store3.OpenAsyncSession())
        {
            var u1 = await s1.LoadAsync<object>("users/1");
            var u2 = await s2.LoadAsync<object>("users/1");
            var u3 = await s3.LoadAsync<object>("users/1");
            
            Assert.Equal(s1.Advanced.GetChangeVectorFor(u1), s2.Advanced.GetChangeVectorFor(u2));
            Assert.Equal(s2.Advanced.GetChangeVectorFor(u2), s3.Advanced.GetChangeVectorFor(u3));
        }
    }
}
