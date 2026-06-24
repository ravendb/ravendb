using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26846 : RavenTestBase
{
    public RavenDB_26846(ITestOutputHelper output) : base(output)
    {
    }

    [RavenTheory(RavenTestCategory.ClientApi)]
    [InlineData(0UL)]
    [InlineData(5UL)]
    [InlineData(9007199254740992UL)] // 2^53 - largest integer exactly representable as a double
    [InlineData(9007199254740993UL)] // 2^53 + 1 - first ulong that loses precision when bridged through a double
    [InlineData(1419814379775459300UL)] // the value from the bug report
    [InlineData(9223372036854775807UL)] // long.MaxValue - largest value parsed back as an Integer token
    [InlineData(9223372036854775808UL)] // long.MaxValue + 1 - first value that must be stored as a LazyNumber
    [InlineData(ulong.MaxValue)] // 18446744073709551615
    public async Task HasChanges_ShouldBeFalse_AfterLoadingEntityWithULongProperty(ulong value)
    {
        using var store = GetDocumentStore();

        const string id = "data/1";

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new DataWithULong { Id = id, Value = value });
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var loaded = await session.LoadAsync<DataWithULong>(id);

            Assert.NotNull(loaded);
            Assert.Equal(value, loaded.Value);

            // loading an unmodified entity must leave the session clean
            Assert.Empty(session.Advanced.WhatChangedFor(loaded));
            Assert.False(session.Advanced.HasChanges);
        }
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task HasChanges_ShouldStillDetectModification_ForULongProperty()
    {
        using var store = GetDocumentStore();

        const string id = "data/1";
        const ulong original = 1419814379775459300UL;

        using (var session = store.OpenAsyncSession())
        {
            await session.StoreAsync(new DataWithULong { Id = id, Value = original });
            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var loaded = await session.LoadAsync<DataWithULong>(id);
            Assert.False(session.Advanced.HasChanges);

            loaded.Value = original + 1;

            Assert.True(session.Advanced.HasChanges);
            Assert.NotEmpty(session.Advanced.WhatChangedFor(loaded));

            await session.SaveChangesAsync();
        }

        using (var session = store.OpenAsyncSession())
        {
            var loaded = await session.LoadAsync<DataWithULong>(id);

            Assert.Equal(original + 1, loaded.Value);
            Assert.False(session.Advanced.HasChanges);
        }
    }

    private class DataWithULong
    {
        public string Id { get; set; }

        public ulong Value { get; set; }
    }
}
