using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
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

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries)]
        public async Task DeleteTimeSeriesShouldNotThrowNotSupportedException_mapReduceIndexWithOutputToCollection()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation(operateOnTypes: _operateOnTypes));

                var indexName = "Companies/StockPrices/TradeVolumeByMonth";

                await WaitAndAssertForValueAsync(async () =>
                {
                    var indexNames = await store.Maintenance.SendAsync(new GetIndexNamesOperation(start: 0, pageSize: int.MaxValue));
                    return indexNames.Contains(indexName, StringComparer.OrdinalIgnoreCase);
                }, true);

                // add OutputReduceToCollection
                var index = await store.Maintenance.SendAsync(new GetIndexOperation(indexName));
                index.OutputReduceToCollection = "StockPricesTradeVolumeByMonth";
                await store.Maintenance.SendAsync(new PutIndexesOperation(index));

                try
                {
                    Indexes.WaitForIndexing(store);
                }
                catch
                {
                    // do nothing...
                }

                var indexErrors = await store.Maintenance.SendAsync(new GetIndexErrorsOperation(new[] { indexName }));

                Assert.Single(indexErrors);
                Assert.Empty(indexErrors[0].Errors);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i < 5; i++)
                        session.Delete($"companies/{i}-A");

                    await session.SaveChangesAsync();
                }

                try
                {
                    Indexes.WaitForIndexing(store);
                }
                catch
                {
                    // do nothing...
                }

                indexErrors = await store.Maintenance.SendAsync(new GetIndexErrorsOperation(new[] { indexName }));

                Assert.Single(indexErrors);
                Assert.Empty(indexErrors[0].Errors);
            }
        }

        [RavenFact(RavenTestCategory.Indexes | RavenTestCategory.TimeSeries)]
        public async Task DeleteTimeSeriesShouldNotThrowNotSupportedException_mapReduceIndexWithOutputToCollection2()
        {
            using (var store = GetDocumentStore())
            {
                await store.Maintenance.SendAsync(new CreateSampleDataOperation(operateOnTypes: _operateOnTypes));

                var indexName = "Companies/StockPrices/TradeVolumeByMonth";

                await WaitAndAssertForValueAsync(async () =>
                {
                    var indexNames = await store.Maintenance.SendAsync(new GetIndexNamesOperation(start: 0, pageSize: int.MaxValue));
                    return indexNames.Contains(indexName, StringComparer.OrdinalIgnoreCase);
                }, true);

                // add OutputReduceToCollection
                var index = await store.Maintenance.SendAsync(new GetIndexOperation(indexName));
                index.OutputReduceToCollection = "StockPricesTradeVolumeByMonth";
                await store.Maintenance.SendAsync(new PutIndexesOperation(index));

                try
                {
                    Indexes.WaitForIndexing(store);
                }
                catch
                {
                    // do nothing...
                }

                var indexErrors = await store.Maintenance.SendAsync(new GetIndexErrorsOperation(new[] { indexName }));

                Assert.Single(indexErrors);
                Assert.Empty(indexErrors[0].Errors);

                using (var session = store.OpenAsyncSession())
                {
                    for (int i = 1; i < 5; i++)
                        session.TimeSeriesFor($"companies/{i}-A", "StockPrices").Delete();

                    await session.SaveChangesAsync();
                }

                try
                {
                    Indexes.WaitForIndexing(store);
                }
                catch
                {
                    // do nothing...
                }

                indexErrors = await store.Maintenance.SendAsync(new GetIndexErrorsOperation(new[] { indexName }));

                Assert.Single(indexErrors);
                Assert.Equal(indexName, indexErrors[0].Name, StringComparer.OrdinalIgnoreCase);
                Assert.Empty(indexErrors[0].Errors);
            }
        }
    }
}
