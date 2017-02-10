using FastTests.Server.Documents.Queries.Dynamic.MapReduce;
using Raven.Client;
using Raven.Client.Indexing;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Xunit;

namespace SlowTests.Server.Documents.Queries
{
    public class MatchingAutoMapRediceIndexesForDynamicQueriesSlow : MatchingAutoMapReduceIndexesForDynamicQueries
    {

        [Fact]
        public void Failure_if_matching_index_has_lot_of_errors()
        {
            var definition = new AutoMapReduceIndexDefinition("Users", new[]
             {
                new IndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                    MapReduceOperation = FieldMapReduceOperation.Count,
                },
            },
             new[]
             {
                new IndexField
                {
                    Name = "Location",
                    Storage = FieldStorage.Yes,
                }
             });

            add_index(definition);

            get_index(definition.Name)._indexStorage.UpdateStats(SystemTime.UtcNow, new IndexingRunStats
            {
                MapAttempts = 1000,
                MapSuccesses = 1000,
                ReduceAttempts = 1000,
                ReduceErrors = 900
            });

            var dynamicQuery = DynamicQueryMapping.Create("Users", new IndexQueryServerSide
            {
                Query = "",
                DynamicMapReduceFields = new[]
                {
                    new DynamicMapReduceField
                    {
                        Name = "Count",
                        OperationType = FieldMapReduceOperation.Count
                    },
                    new DynamicMapReduceField
                    {
                        Name = "Location",
                        IsGroupBy = true
                    }
                }
            });

            var result = _sut.Match(dynamicQuery);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }
    }
}