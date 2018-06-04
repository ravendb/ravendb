using FastTests;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Xunit;

namespace SlowTests.Server.Documents.Queries.Dynamic.MapReduce
{
    public class RavenDB_8075 : RavenLowLevelTestBase
    {
        [Fact]
        public void Should_match_auto_map_reduce_index_if_analyzed_field_isnt_used_in_where()
        {
            using (var db = CreateDocumentDatabase())
            {
                var mapping = DynamicQueryMapping.Create(new IndexQueryServerSide(@"
from LastFms
group by Artist
where search(Artist, ""Rapper"")
order by Count as long desc
select count() as Count, Artist"));

                db.IndexStore.CreateIndex(mapping.CreateAutoIndexDefinition()).Wait();

                mapping = DynamicQueryMapping.Create(new IndexQueryServerSide(@"
from LastFms
group by Artist
where Count > 100
order by Count as long desc
select count() as Count, Artist"));

                var matcher = new DynamicQueryToIndexMatcher(db.IndexStore);

                var result = matcher.Match(mapping, null);

                Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            }
        }

        [Fact]
        public void Should_match_auto_map_index_if_analyzed_field_isnt_used_in_where()
        {
            using (var db = CreateDocumentDatabase())
            {
                var mapping = DynamicQueryMapping.Create(new IndexQueryServerSide(@"
from LastFms
where search(Artist, ""Chri"") and Genre = ""jazz"""));

                db.IndexStore.CreateIndex(mapping.CreateAutoIndexDefinition()).Wait();

                mapping = DynamicQueryMapping.Create(new IndexQueryServerSide(@"
from LastFms
where Genre = ""jazz""
select Artist"));

                var matcher = new DynamicQueryToIndexMatcher(db.IndexStore);

                var result = matcher.Match(mapping, null);

                Assert.Equal(DynamicQueryMatchType.Complete, result.MatchType);
            }
        }
    }
}
