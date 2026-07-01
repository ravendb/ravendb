using FastTests;
using Raven.Server.Documents.Queries;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_25281_MetadataCacheCollision : NoDisposalNeeded
    {
        public RavenDB_25281_MetadataCacheCollision(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Querying)]
        public void TryGetMetadata_returning_false_must_not_leak_another_querys_metadata()
        {
            // Regression for the GET query-path metadata mix-up: TryGetMetadata used to leave `out metadata`
            // pointing at the entry sitting in a probed slot when it returned false (hash-slot collision, the
            // probed query's text did not match). A caller that only null-checked the out value then executed
            // one query against another query's cached metadata - e.g. binding $p from a `runtime > $r` query.
            // The trigger needs no 64-bit hash collision: a primary slot occupied by a different key forces the
            // probe branch, and an occupied probe slot was leaked. The contract is simply: false => metadata null.
            var cache = new QueryMetadataCache();

            // Fill a large fraction of the 512-slot cache so primary- and probe-slot collisions are common.
            for (int i = 0; i < 400; i++)
            {
                var text = $"from Users where Age = {i} order by Name";
                Assert.False(cache.TryGetMetadata(new IndexQueryServerSide(text), addSpatialProperties: false, out var hash, out _));
                cache.MaybeAddToCache(new QueryMetadata(text, null, hash), "test");
            }

            // Probe with many queries whose text is NOT in the cache. Every lookup must miss (text differs), and
            // every miss must hand back a null metadata - never a colliding entry from a probed slot.
            for (int i = 0; i < 4000; i++)
            {
                var text = $"from Orders where Total = {i} order by Company";
                bool found = cache.TryGetMetadata(new IndexQueryServerSide(text), addSpatialProperties: false, out _, out var metadata);

                Assert.False(found);
                Assert.Null(metadata);
            }
        }
    }
}
