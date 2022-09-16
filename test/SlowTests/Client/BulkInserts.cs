
using System.IO.Compression;
using System.Threading.Tasks;
using FastTests;
using Tests.Infrastructure;
using xRetry;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class BulkInserts : NoDisposalNoOutputNeeded
    {
        public BulkInserts(ITestOutputHelper output) : base(output)
        {
        }

        [RetryTheory]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task Simple_Bulk_Insert_With_Ssl(RavenTestBase.Options options)
        {
            using (var x = new FastTests.Client.BulkInserts(Output))
            {
                await x.Simple_Bulk_Insert(options, useSsl: true, compressionLevel: CompressionLevel.NoCompression);
            }
        }
    }
}
