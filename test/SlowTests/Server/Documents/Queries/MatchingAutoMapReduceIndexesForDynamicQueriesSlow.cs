using System;
using System.Runtime.CompilerServices;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Util;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce.Auto;
using Raven.Server.Documents.Queries;
using Raven.Server.Documents.Queries.Dynamic;
using Tests.Infrastructure;
using Xunit;
using Index = Raven.Server.Documents.Indexes.Index;

namespace SlowTests.Server.Documents.Queries
{
    public class MatchingAutoMapReduceIndexesForDynamicQueriesSlow : RavenLowLevelTestBase
    {
        public MatchingAutoMapReduceIndexesForDynamicQueriesSlow(ITestOutputHelper output) : base(output)
        {
        }

        private DocumentDatabase _documentDatabase;
        protected DynamicQueryToIndexMatcher _sut;
        private SearchEngineType DefaultAutoIndexingEngineType => _documentDatabase.Configuration.Indexing.AutoIndexingEngineType;

        public void Initialize([CallerMemberName] string caller = null)
        {
            _documentDatabase = CreateDocumentDatabase(caller: caller);

            _sut = new DynamicQueryToIndexMatcher(_documentDatabase.IndexStore);
        }

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
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

            get_index(definition.Name)._indexStorage.UpdateStats(SystemTime.UtcNow, TimeSpan.Zero, new IndexingRunStats
            {
                MapAttempts = 1000,
                MapSuccesses = 1000,
                ReduceAttempts = 1000,
                ReduceErrors = 900
            });

            var dynamicQuery = DynamicQueryMapping.Create(new IndexQueryServerSide("FROM Users GROUP BY Location SELECT count() "), DefaultAutoIndexingEngineType);

            var result = _sut.Match(dynamicQuery,  null);

            Assert.Equal(DynamicQueryMatchType.Failure, result.MatchType);
        }

        protected void add_index(IndexDefinitionBaseServerSide definition)
        {
            AsyncHelpers.RunSync(() => _documentDatabase.IndexStore.CreateIndex(definition, Guid.NewGuid().ToString()));
        }

        protected Index get_index(string name)
        {
            return _documentDatabase.IndexStore.GetIndex(name);
        }

        public override async ValueTask DisposeAsync()
        {
            try
            {
                _documentDatabase.Dispose();
            }
            finally
            {
                await base.DisposeAsync();
            }
        }
    }
}
