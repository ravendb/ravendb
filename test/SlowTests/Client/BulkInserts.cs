
using System.IO.Compression;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Http;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.Client
{
    public class BulkInserts : NoDisposalNoOutputNeeded
    {
        public BulkInserts(ITestOutputHelper output) : base(output)
        {
        }

        [RavenRetryTheory(RavenTestCategory.BulkInsert)]
        [RavenData(CompressionLevel.NoCompression, HttpCompressionAlgorithm.Gzip, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(CompressionLevel.NoCompression, HttpCompressionAlgorithm.Zstd, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(CompressionLevel.Optimal, HttpCompressionAlgorithm.Gzip, DatabaseMode = RavenDatabaseMode.All)]
        [RavenData(CompressionLevel.Optimal, HttpCompressionAlgorithm.Zstd, DatabaseMode = RavenDatabaseMode.All)]
        public async Task Simple_Bulk_Insert_With_Ssl(RavenTestBase.Options options, CompressionLevel compressionLevel, HttpCompressionAlgorithm compressionAlgorithm)
        {
            using (var x = new FastTests.Client.BulkInserts(Output))
            {
                await x.Simple_Bulk_Insert(options, useSsl: true, compressionLevel, compressionAlgorithm);
            }
        }
    }
}
