using System.Threading.Tasks;
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
            using (var x = new FastTests.Client.BulkInserts(Output))
            {
                await x.Simple_Bulk_Insert(useSsl: true, "1.1");
            }
        }

        [Fact]
        public async Task Simple_Bulk_Insert_With_Ssl_http_2()
        {
            using (var x = new FastTests.Client.BulkInserts(Output))
            {
                await x.Simple_Bulk_Insert(useSsl: true, "2.0");
            }
        }
    }
}
