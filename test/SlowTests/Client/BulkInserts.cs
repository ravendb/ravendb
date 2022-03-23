
using System.IO.Compression;
using System.Threading.Tasks;
using FastTests;
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

        [Theory]
        [RavenData(false, DatabaseMode = RavenDatabaseMode.All)]
        public async Task Simple_Bulk_Insert_With_Ssl(RavenTestBase.Options options)
        {
            using (var x = new FastTests.Client.BulkInserts(Output))
            {
                await x.Simple_Bulk_Insert(options, useSsl: true, compressionLevel: CompressionLevel.NoCompression);
            }
        }
    }
}
