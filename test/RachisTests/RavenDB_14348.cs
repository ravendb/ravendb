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
            using (var store = GetDocumentStore())
            {
                var task = Task.Run(async () =>
                {
                    while (false == store.WasDisposed)
                    {
                        await SubscriptionFailoverWithWaitingChains.ContinuouslyGenerateDocsInternal(10, store);
                    }
                });

                await Task.Delay(5555);
            }
        }
    }
}
