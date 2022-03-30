using System.Threading.Tasks;
using Raven.Client.Http;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class BulkInserts : NoDisposalNoOutputNeeded
    {
        public BulkInserts(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Simple_Bulk_Insert_With_Ssl()
        {
            try
            {
                NodeSelector.ShowDebugMessages = true;
                using (var x = new FastTests.Client.BulkInserts(Output))
                {
                    await x.Simple_Bulk_Insert(useSsl: true);
                }
            }
            finally
            {
                NodeSelector.ShowDebugMessages = false;
            }
        }
    }
}
