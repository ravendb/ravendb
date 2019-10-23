using System.Threading.Tasks;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class BulkInserts : NoDisposalNeeded
    {
        public BulkInserts(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Simple_Bulk_Insert_With_Ssl()
        {
            using (var x = new FastTests.Client.BulkInserts(Output))
            {
                await x.Simple_Bulk_Insert(useSsl: true);
            }
        }
    }
}
