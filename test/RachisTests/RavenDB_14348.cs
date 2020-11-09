using System;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace RachisTests
{
    public class RavenDB_14348 : RavenTestBase
    {
        public RavenDB_14348(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task ShouldWork()
        {
            using (var cts = new CancellationTokenSource(TimeSpan.FromMinutes(5)))
            using (var store = GetDocumentStore())
            {
                _ = Task.Run(async () =>
                {
                    while (false == store.WasDisposed)
                    {
                        cts.Token.ThrowIfCancellationRequested();

                        await SubscriptionFailoverWithWaitingChains.ContinuouslyGenerateDocsInternal(10, store, cts.Token);
                    }
                }, cts.Token);

                await Task.Delay(5555, cts.Token);
            }
        }
    }
}
