using System.Runtime.CompilerServices;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Xunit;

namespace SlowTests.Server.Documents.Queries
{
    public class MatchingAutoMapReduceIndexesForDynamicQueriesSlow : RavenLowLevelTestBase
    {
        private DocumentDatabase _documentDatabase;
        protected DynamicQueryToIndexMatcher _sut;

        public void Initialize([CallerMemberName] string caller = null)
        {
            _documentDatabase = CreateDocumentDatabase(caller: $"{GetType().Name}.{caller}");

            _sut = new DynamicQueryToIndexMatcher(_documentDatabase.IndexStore);
        }

        [Fact]
        public void Failure_if_matching_index_has_lot_of_errors()
        {
            Initialize();
            var definition = new AutoMapReduceIndexDefinition("Users", new[]
             {
                new AutoIndexField
                {
                    Name = "Count",
                    Storage = FieldStorage.Yes,
                    Aggregation = AggregationOperation.Count,
                },
            },
             new[]
             {
                new AutoIndexField
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

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users GROUP BY Location SELECT count() "));

            var result = _sut.Match(dynamicQuery,  null);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        protected void add_index(IndexDefinitionBase definition)
        {
            AsyncHelpers.RunSync(() => _documentDatabase.IndexStore.CreateIndex(definition));
        }

        protected Index get_index(string name)
        {
            return _documentDatabase.IndexStore.GetIndex(name);
        }

        public override void Dispose()
        {
            try
            {
                _documentDatabase.Dispose();
            }
            finally
            {
                base.Dispose();
            }
        }
    }
}
