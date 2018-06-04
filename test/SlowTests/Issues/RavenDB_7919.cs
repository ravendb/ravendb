using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.Auto;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Raven.Server.ServerWide;
using Raven.Server.ServerWide.Context;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7919 : RavenLowLevelTestBase
    {
        [Fact]
        public async Task Should_use_auto_index_even_if_idle_when_match_is_complete()
        {
            using (var database = CreateDocumentDatabase())
            {
                var autoIndex = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[]
                {
                    new AutoIndexField
                    {
                        Name = "FirstName",
                    },
                    new AutoIndexField
                    {
                        Name = "LastName",
                    }
                }));

                autoIndex.SetState(IndexState.Idle);

                using (var context = DocumentsOperationContext.ShortTermSingleUse(database))
                {
                    // it shouldn't throw
                    await database.QueryRunner.ExecuteQuery(new IndexQueryServerSide("from Users where LastName = 'Arek'"), context, null,
                        OperationCancelToken.None);
                }

                var sameIndex = database.IndexStore.GetIndex(autoIndex.Name);

                Assert.Same(autoIndex, sameIndex);
                Assert.Equal(IndexState.Normal, sameIndex.State);
            }
        }

        [Fact]
        public async Task Complete_but_idle_match_if_auto_map_index_is_idle()
        {
            using (var database = CreateDocumentDatabase())
            {
                var matcher = new DynamicQueryToIndexMatcher(database.IndexStore);

                var autoIndex = await database.IndexStore.CreateIndex(new AutoMapIndexDefinition("Users", new[]
                {
                    new AutoIndexField
                    {
                        Name = "FirstName",
                    }
                }));

                autoIndex.SetState(IndexState.Idle);

                var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("from Users where FirstName = 'Arek'"));

                var result = matcher.Match(dynamicQuery, null);

                Assert.Equal(DynamicQueryMatchType.CompleteButIdle, result.MatchType);
            }
        }

        [Fact]
        public async Task Complete_but_idle_match_if_auto_map_reduce_index_is_idle()
        {
            using (var database = CreateDocumentDatabase())
            {
                var matcher = new DynamicQueryToIndexMatcher(database.IndexStore);

                var autoIndex = await database.IndexStore.CreateIndex(new AutoMapReduceIndexDefinition("Users", new[]
                {
                    new AutoIndexField
                    {
                        Name = "Count",
                        Aggregation = AggregationOperation.Count
                    }
                },
                new[]
                {
                    new AutoIndexField
                    {
                        Name = "Location",
                    }
                }));

                autoIndex.SetState(IndexState.Idle);

                var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("from Users group by Location select count()"));

                var result = matcher.Match(dynamicQuery, null);

                Assert.Equal(DynamicQueryMatchType.CompleteButIdle, result.MatchType);
            }
        }
    }
}
