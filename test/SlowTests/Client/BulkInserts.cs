using System.Threading.Tasks;
using FastTests;
using Xunit;

namespace SlowTests.Client
{
    public class BulkInserts : RavenTestBase
    {
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
