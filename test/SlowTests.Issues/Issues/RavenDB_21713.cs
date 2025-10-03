using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_21713 : RavenTestBase
{
    public RavenDB_21713(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task Should_Not_Throw_AVE()
    {
        using (var store = GetDocumentStore())
        {
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Item(), "items/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var item = await session.LoadAsync<Item>("items/1");
                item.List = new[] { "1", "2" };

                var changes = session.Advanced.WhatChangedFor(item);
                Assert.Equal(1, changes.Length);
                _ = changes[0].FieldNewValue.ToString(); // AVE

                await session.SaveChangesAsync();
            }
        }
    }

    private class Item
    {
        public string[] List { get; set; }
    }
}
