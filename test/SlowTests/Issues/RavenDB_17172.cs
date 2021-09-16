using FastTests;
using Raven.Server.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_17172 : NoDisposalNeeded
    {
        public RavenDB_17172(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void Should_not_cache_query_metadata_if_addSpatialProperties_is_set_to_true()
        {
            var cache = new QueryMetadataCache();

            Assert.False(cache.TryGetMetadata(new IndexQueryServerSide("from Users order by Name"), addSpatialProperties: false, out var originalMetadataHash, out _));

            Assert.NotEqual((ulong)0, originalMetadataHash);

            cache.MaybeAddToCache(new QueryMetadata("from Users order by Name", null, cacheKey: originalMetadataHash, addSpatialProperties: true), "test");

            Assert.False(cache.TryGetMetadata(new IndexQueryServerSide("from Users order by Name"), addSpatialProperties: false, out var metadataHash, out _));

            Assert.False(cache.TryGetMetadata(new IndexQueryServerSide("from Users order by Name"), addSpatialProperties: true, out metadataHash, out _));

            cache.MaybeAddToCache(new QueryMetadata("from Users order by Name", null, cacheKey: originalMetadataHash, addSpatialProperties: false), "test");

            Assert.True(cache.TryGetMetadata(new IndexQueryServerSide("from Users order by Name"), addSpatialProperties: false, out metadataHash, out _));

            Assert.False(cache.TryGetMetadata(new IndexQueryServerSide("from Users order by Name"), addSpatialProperties: true, out metadataHash, out _));
        }
    }
}
