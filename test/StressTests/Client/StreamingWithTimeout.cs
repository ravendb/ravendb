using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Session;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace StressTests.Client
{
    public class StreamingWithTimeout : RavenTestBase
    {
        [Fact]
        public async Task TimeoutOnHangingStreamQuery()
        {
            using (var store = GetDocumentStore())
            {
                const int numberOfItems = 10_000;

                using (var bulkInsert = store.BulkInsert())
                {
                    for (var i = 0; i < numberOfItems; i++)
                        await bulkInsert.StoreAsync(new Order());
                }

                var database = await GetDocumentDatabaseInstanceFor(store);

                using (var session = store.OpenAsyncSession(new SessionOptions { NoCaching = true, NoTracking = true }))
                {
                    var results = await session.Advanced.StreamAsync(session.Query<Order>());

                    var count = 0;
                    while (await results.MoveNextAsync())
                    {
                        var order = results.Current;
                        count++;
                    }

                    Assert.Equal(numberOfItems, count);
                }

                using (var session = store.OpenAsyncSession(new SessionOptions { NoCaching = true, NoTracking = true }))
                {
                    var count = 0;
                    var testing = database.DocumentsStorage.ForTestingPurposesOnly();

                    try
                    {
                        await Assert.ThrowsAsync<TaskCanceledException>(async () =>
                        {
                            var results = await session.Advanced.StreamAsync(session.Query<Order>());

                            while (await results.MoveNextAsync())
                            {
                                var order = results.Current;
                                count++;

                                if (testing.DelayDocumentLoad == null)
                                    testing.DelayDocumentLoad = new ManualResetEventSlim(false);
                            }
                        });
                    }
                    finally
                    {
                        Assert.NotEqual(numberOfItems, count);

                        testing.DelayDocumentLoad?.Set();
                    }
                }
            }
        }

        public StreamingWithTimeout(ITestOutputHelper output) : base(output)
        {
        }
    }
}
