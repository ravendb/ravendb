using FastTests;
using Raven.Server.Documents.Queries;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_11284 : NoDisposalNeeded
    {
        [Fact]
        public void Should_cache_metadata_of_queries_without_parameters()
        {
            var cache = new QueryMetadataCache();

            Assert.False(cache.TryGetMetadata(new IndexQueryServerSide("from Users order by Name"), out var metadataHash, out var metadata));

            Assert.NotEqual((ulong)0, metadataHash);

            cache.MaybeAddToCache(new QueryMetadata("from Users order by Name", null, metadataHash), "test");

            Assert.True(cache.TryGetMetadata(new IndexQueryServerSide("from Users order by Name"), out metadataHash, out metadata));
        }
    }
}
