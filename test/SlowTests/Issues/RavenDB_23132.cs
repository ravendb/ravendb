using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Smuggler;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_23132 : RavenTestBase
    {
        private readonly DatabaseItemType _operateOnTypes = DatabaseItemType.Documents |
                                                            DatabaseItemType.Indexes |
                                                            DatabaseItemType.TimeSeries;

        public RavenDB_23132(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries)]
        [RavenData(Data = [true], SearchEngineMode = RavenSearchEngineMode.All)]
        [RavenData(Data = [false], SearchEngineMode = RavenSearchEngineMode.All)]
        public async Task DeleteTimeSeriesShouldNotThrowNotSupportedException_mapReduceIndexWithOutputToCollection(Options options, bool deleteDocument)
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation(operateOnTypes: _operateOnTypes));

                var indexName = "Companies/StockPrices/TradeVolumeByMonth";
                var outputToCollection = "StockPricesTradeVolumeByMonth";

                await WaitAndAssertForValueAsync(async () =>
                {
                    var indexNames = await store.Maintenance.SendAsync(new GetIndexNamesOperation(start: 0, pageSize: int.MaxValue));
                    return indexNames.Contains(indexName, StringComparer.OrdinalIgnoreCase);
                }, true);

                // add OutputReduceToCollection
                var index = await store.Maintenance.SendAsync(new GetIndexOperation(indexName));
                index.OutputReduceToCollection = outputToCollection;
                await store.Maintenance.SendAsync(new PutIndexesOperation(index));

                
                Indexes.WaitForIndexing(store, allowErrors: true);

                var collectionStats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());
               
                Assert.True(collectionStats.Collections.TryGetValue(outputToCollection, out var outputs1));
                Assert.True(outputs1 > 0);

                var indexErrors = await store.Maintenance.SendAsync(new GetIndexErrorsOperation(new[] { indexName }));

                Assert.Single(indexErrors);
                Assert.Empty(indexErrors[0].Errors);

                using (var session = store.OpenAsyncSession())
                {
                    if (deleteDocument)
                    {
                        for (int i = 1; i <= 5; i++)
                            session.Delete($"companies/{i}-A");
                    }
                    else
                    {
                        for (int i = 1; i <= 5; i++)
                            session.TimeSeriesFor($"companies/{i}-A", "StockPrices").Delete();
                    }

                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store, allowErrors: true);

                collectionStats = await store.Maintenance.SendAsync(new GetCollectionStatisticsOperation());

                Assert.True(collectionStats.Collections.TryGetValue(outputToCollection, out var outputs2));
                Assert.True(outputs2 < outputs1);

                indexErrors = await store.Maintenance.SendAsync(new GetIndexErrorsOperation(new[] { indexName }));

                Assert.Single(indexErrors);
                Assert.Empty(indexErrors[0].Errors);
            }
        }
    }
}
