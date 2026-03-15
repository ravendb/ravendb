using System.IO.Compression;
using System.Threading.Tasks;
using Raven.Client.Documents.BulkInsert;
using Raven.Client.Http;
using Raven.Embedded;
using Xunit;

namespace EmbeddedTests
{
    public class BulkInsertTests : EmbeddedTestBase
    {
        [Theory]
        [InlineData(CompressionLevel.NoCompression, HttpCompressionAlgorithm.Gzip)]
        [InlineData(CompressionLevel.Optimal, HttpCompressionAlgorithm.Gzip)]
        public async Task Simple_Bulk_Insert(CompressionLevel compressionLevel, HttpCompressionAlgorithm compressionAlgorithm)
        {
            var options = CopyServerAndCreateOptions();

            using (var embedded = new EmbeddedServer())
            {
                embedded.StartServer(options);

                using (var store = embedded.GetDocumentStore(new DatabaseOptions("TestBulkInsert")
                {
                    Conventions = new Raven.Client.Documents.Conventions.DocumentConventions
                    {
                        HttpCompressionAlgorithm = compressionAlgorithm
                    }
                }))
                {
                    using (var bulkInsert = store.BulkInsert(new BulkInsertOptions
                    {
                        CompressionLevel = compressionLevel
                    }))
                    {
                        for (int i = 0; i < 1000; i++)
                        {
                            await bulkInsert.StoreAsync(new FooBar
                            {
                                Name = "foobar/" + i
                            }, "FooBars/" + i);
                        }
                    }

                    using (var session = store.OpenSession())
                    {
                        var len = session.Advanced.LoadStartingWith<FooBar>("FooBars/", null, 0, 1000, null);
                        Assert.Equal(1000, len.Length);
                    }
                }
            }
        }

        private class FooBar
        {
            public string Name { get; set; }
        }
    }
}
