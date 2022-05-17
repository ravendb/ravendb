using System.Diagnostics.CodeAnalysis;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Documents.Indexing.Auto;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Queries;
using Raven.Server.ServerWide;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.Indexing.Auto
{
    [SuppressMessage("ReSharper", "ConsiderUsingConfigureAwait")]
    public class BasicAutoMapReduceIndexingSlow : RavenLowLevelTestBase
    {
        public BasicAutoMapReduceIndexingSlow(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public async Task MultipleAggregationFunctionsCanBeUsed()
        {
            using (var db = CreateDocumentDatabase())
            using (var mri = AutoMapReduceIndex.CreateNew(new AutoMapReduceIndexDefinition("Users", new[]
            {
                new AutoIndexField
                {
                    Name = "Count",
                    Aggregation = AggregationOperation.Count,
                    Storage = FieldStorage.Yes
                },
                new AutoIndexField
                {
                    Name = "TotalCount",
                    Aggregation = AggregationOperation.Count,
                    Storage = FieldStorage.Yes
                },
                new AutoIndexField
                {
                    Name = "Age",
                    Aggregation = AggregationOperation.Sum,
                    Storage = FieldStorage.Yes
                }
            }, new[]
            {
                new AutoIndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes
                },
            }), db))
            {
                BasicAutoMapReduceIndexing.CreateUsers(db, 2, "Poland");

                mri.DoIndexingWork(new IndexingStatsScope(new IndexingRunStats()), CancellationToken.None);

                using (var context = QueryOperationContext.ShortTermSingleUse(db))
                {
                    var queryResult = await mri.Query(new IndexQueryServerSide($"FROM INDEX '{mri.Name}'"), context, OperationCancelToken.None);

                    Assert.Equal(1, queryResult.Results.Count);
                    var result = queryResult.Results[0].Data;

                    string location;
                    Assert.True(result.TryGet("Location", out location));
                    Assert.Equal("Poland", location);

                    Assert.Equal(2L, result["Count"]);

                    Assert.Equal(2L, result["TotalCount"]);

                    Assert.Equal(41L, result["Age"]);
                }

                using (var context = QueryOperationContext.ShortTermSingleUse(db))
                {
                    var queryResult = await mri.Query(new IndexQueryServerSide($"FROM INDEX '{mri.Name}' WHERE Count BETWEEN 2 AND 10"), context,
                        OperationCancelToken.None);

                    Assert.Equal(1, queryResult.Results.Count);
                }

                using (var context = QueryOperationContext.ShortTermSingleUse(db))
                {
                    var queryResult = await mri.Query(new IndexQueryServerSide($"FROM INDEX '{mri.Name}' WHERE Count >= 10"), context, OperationCancelToken.None);

                    Assert.Equal(0, queryResult.Results.Count);
                }
            }
        }

    }
}
