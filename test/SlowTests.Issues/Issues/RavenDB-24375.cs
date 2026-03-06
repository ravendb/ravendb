using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_24375(ITestOutputHelper output) : RavenTestBase(output)
{
    [RavenTheory(RavenTestCategory.Core | RavenTestCategory.Patching)]
    [InlineData(2047, 1023)]
    [InlineData(2047, 1024)]
    [InlineData(1025, 1023)]
    [InlineData(1025, 1024)]
    public async Task CanDeleteByQueryFromCollectionWhenSkipOffsetIsAtLeast1024(int size, int offset)
    {
        using var store = GetDocumentStore();

        await using (var bulkInsert = store.BulkInsert())
        {
            for (int i = 0; i < size; i++)
                await bulkInsert.StoreAsync(new Dto(i));
        }

        Operation deleteOperation = await store
            .Operations
            .SendAsync(new DeleteByQueryOperation($"from Dtos limit {offset}, {size}"));

        await deleteOperation.WaitForCompletionAsync(TimeSpan.FromMinutes(15));

        using (var session = store.OpenAsyncSession())
        {
            var count = await session.Query<Dto>().CountAsync();
            Assert.Equal(offset, count);
        }
    }

#pragma warning disable CS9113 // Parameter is unread.
    // ReSharper disable once InconsistentNaming
    private struct Dto(long Idx, string Id = null);
#pragma warning restore CS9113 // Parameter is unread.
}
