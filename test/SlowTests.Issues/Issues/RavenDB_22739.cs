using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues;

public class RavenDB_22739 : RavenTestBase
{
    public RavenDB_22739(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.ClientApi)]
    public async Task AggressiveCacheChangeNotificationShouldInvalidateCache()
    {
        using var store1 = GetDocumentStore();
        using var store2 = new DocumentStore() { Urls = store1.Urls, Database = store1.Database }.Initialize();
        using var store3 = new DocumentStore() { Urls = store1.Urls, Database = store1.Database }.Initialize();
        using var store4 = new DocumentStore() { Urls = store1.Urls, Database = store1.Database }.Initialize();

        var stores = new[] { store1, store2, store3, store4 };

        using (var session = store1.OpenSession())
        {
            session.Store(new Item("Joe"), "items/1");
            session.SaveChanges();
        }

        foreach (var store in stores)
        {
            using (await store.AggressivelyCacheAsync())
            using (var session = store1.OpenSession())
            {
                var item = session.Load<Item>("items/1");

                Assert.NotNull(item);
            }
        }

        using (var session = store1.OpenSession())
        {
            session.Delete("items/1");
            session.SaveChanges();
        }

        foreach (var store in stores)
        {
            using (await store.AggressivelyCacheAsync())
            {
                var retries = 3;

                Item item;

                do
                {
                    using (var session = store.OpenSession())
                    {
                        item = session.Load<Item>("items/1");

                        if (item is null)
                            break;

                        Thread.Sleep(500);
                    }

                } while (--retries > 0);

                Assert.Null(item);
            }
        }
    }

    private record Item(string Name);
}
