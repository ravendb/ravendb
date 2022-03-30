using System;
using System.Threading.Tasks;
using FastTests;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Queries.Dynamic.MapReduce
{
    public class RavenDB_8075 : RavenLowLevelTestBase
    {
        public RavenDB_8075(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Should_match_auto_map_reduce_index_if_analyzed_field_isnt_used_in_where()
        {
            using (var db = CreateDocumentDatabase())
            {
                var mapping = DynamicQueryMapping.Create(new IndexQueryServerSide(@"
from LastFms
group by Artist
where search(Artist, ""Rapper"")
order by Count as long desc
select count() as Count, Artist"));

                await db.IndexStore.CreateIndex(mapping.CreateAutoIndexDefinition(), Guid.NewGuid().ToString());

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
        public async Task Should_match_auto_map_index_if_analyzed_field_isnt_used_in_where()
        {
            using (var db = CreateDocumentDatabase())
            {
                var mapping = DynamicQueryMapping.Create(new IndexQueryServerSide(@"
from LastFms
where search(Artist, ""Chri"") and Genre = ""jazz"""));

                await db.IndexStore.CreateIndex(mapping.CreateAutoIndexDefinition(), Guid.NewGuid().ToString());

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
