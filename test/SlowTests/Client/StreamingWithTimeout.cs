using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Basic.Entities;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class StreamingWithTimeout : RavenTestBase
    {
        [Fact]
        public async Task TimeoutOnHangingStreamQuery()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                var database = await GetDocumentDatabaseInstanceFor(store);

                using (var session = store.OpenAsyncSession())
                {
                    var results = await session.Advanced.StreamAsync(session.Query<Order>());

                    while (await results.MoveNextAsync())
                    {
                        var order = results.Current;
                    }
                }

                using (var session = store.OpenAsyncSession())
                {
                    await Assert.ThrowsAsync<TaskCanceledException>(async () =>
                    {
                        var results = await session.Advanced.StreamAsync(session.Query<Order>());

                        while (await results.MoveNextAsync())
                        {
                            var order = results.Current;
                            var testing = database.DocumentsStorage.ForTestingPurposesOnly();
                            if (testing.DelayDocumentLoad == null)
                                testing.DelayDocumentLoad = new ManualResetEventSlim(false);
                        }
                    });
                    database.DocumentsStorage.ForTestingPurposesOnly().DelayDocumentLoad.Set();
                }
            }
        }

        public StreamingWithTimeout(ITestOutputHelper output) : base(output)
        {
        }
    }
}
