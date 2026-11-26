using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Xunit;

namespace Allocations.Benchmark
{
    // The map results of static map-reduce indexes, over what their map function itself allocates
    public class MapOutputScenarios(ITestOutputHelper output) : RavenTestBase(output)
    {
        private const int NumberOfOrders = 2_000;
        private const int LinesPerOrder = 3;
        private const string Collection = "Orders";
        private const int Rounds = 3;

        public async Task OrderLinesGroupedByProduct()
        {
            var bytes = await MeasureMapResults(new IndexDefinition
            {
                Name = "Orders/ByProduct",
                Maps =
                {
                    @"from order in docs.Orders
                      from line in order.Lines
                      select new { line.Product, Count = 1, Total = line.Quantity * line.PricePerUnit * (1 - line.Discount) }"
                },
                Reduce = @"from result in results
                           group result by result.Product into g
                           select new { Product = g.Key, Count = g.Sum(x => x.Count), Total = g.Sum(x => x.Total) }"
            }, IndexType.MapReduce, LinesPerOrder);

            Measure.Report("Map results of the lines of an order", bytes, "order");
        }

        public async Task MultiMapIndex()
        {
            var bytes = await MeasureMapResults(new IndexDefinition
            {
                Name = "Orders/ByProductAndEmployee",
                Maps =
                {
                    @"from order in docs.Orders
                      from line in order.Lines
                      select new { Key = line.Product, Count = 1, Total = line.Quantity * line.PricePerUnit * (1 - line.Discount) }",
                    @"from order in docs.Orders
                      select new { Key = order.Employee, Count = 1, Total = 0 }"
                },
                Reduce = @"from result in results
                           group result by result.Key into g
                           select new { Key = g.Key, Count = g.Sum(x => x.Count), Total = g.Sum(x => x.Total) }"
            }, IndexType.MapReduce, LinesPerOrder + 1);

            Measure.Report("Map results of an order", bytes, "order");
        }

        public async Task OrdersGroupedByEmployeeAndMonth()
        {
            var bytes = await MeasureMapResults(new IndexDefinition
            {
                Name = "Orders/ByEmployeeAndMonth",
                Maps =
                {
                    @"from order in docs.Orders
                      select new { order.Employee, Month = order.OrderedAt.ToString(""yyyy-MM""), Count = 1 }"
                },
                Reduce = @"from result in results
                           group result by new { result.Employee, result.Month } into g
                           select new { g.Key.Employee, g.Key.Month, Count = g.Sum(x => x.Count) }"
            }, IndexType.MapReduce, 1);

            Measure.Report("Map result of an order", bytes, "order");
        }

        public async Task JavaScriptIndex()
        {
            var bytes = await MeasureMapResults(new IndexDefinition
            {
                Name = "Orders/ByProduct/JavaScript",
                Maps =
                {
                    @"map('Orders', function (order) {
                          return order.Lines.map(function (line) {
                              return { Product: line.Product, Count: 1, Total: line.Quantity * line.PricePerUnit * (1 - line.Discount) };
                          });
                      })"
                },
                Reduce = @"groupBy(x => x.Product).aggregate(g => {
                               return {
                                   Product: g.key,
                                   Count: g.values.reduce((count, x) => count + x.Count, 0),
                                   Total: g.values.reduce((total, x) => total + x.Total, 0)
                               };
                           })"
            }, IndexType.JavaScriptMapReduce, LinesPerOrder);

            Measure.Report("Map results of the lines of an order", bytes, "order");
        }

        private async Task<double> MeasureMapResults(IndexDefinition definition, IndexType type, int resultsPerOrder)
        {
            using (var store = GetDocumentStore())
            {
                await StoreOrders(store);

                var database = await GetDatabase(store.Database);

                // not added to the index store, so only this thread indexes with it
                using (var index = MapReduceIndex.CreateNew<MapReduceIndex>(definition, database))
                using (var queryContext = QueryOperationContext.Allocate(database, index))
                using (index._contextPool.AllocateOperationContext(out TransactionOperationContext indexContext))
                using (queryContext.OpenReadTransaction())
                using (indexContext.OpenWriteTransaction())
                using (CurrentIndexingScope.Current = index.CreateIndexingScope(indexContext, queryContext))
                using (index.InitializeIndexingWork(indexContext))
                {
                    var stats = new IndexingStatsScope(new IndexingRunStats());
                    var wrapper = new MapReduceIndex.AnonymousObjectToBlittableMapResultsEnumerableWrapper(index, indexContext);

                    long mapped = 0, converted = 0;

                    var mapFunction = Measure.PerItem(() => Map(database, index, type, queryContext, indexContext, stats, mapResults =>
                    {
                        foreach (var _ in mapResults)
                            mapped++;
                    }), NumberOfOrders, Rounds);

                    var bytes = Measure.PerItem(() => Map(database, index, type, queryContext, indexContext, stats, mapResults =>
                    {
                        wrapper.InitializeForEnumeration(mapResults, indexContext, stats);
                        foreach (MapResult result in (IEnumerable<MapResult>)wrapper)
                        {
                            using (result.Data)
                            {
                                if (result.ReduceKeyHash != 0)
                                    converted++;
                            }
                        }
                    }), NumberOfOrders, Rounds);

                    Assert.Equal((Rounds + 1L) * NumberOfOrders * resultsPerOrder, mapped);
                    Assert.Equal(mapped, converted);

                    Measure.Report("Map function", mapFunction, "order");
                    return bytes - mapFunction;
                }
            }
        }

        private static void Map(DocumentDatabase database, MapReduceIndex index, IndexType type, QueryOperationContext queryContext, TransactionOperationContext indexContext,
            IndexingStatsScope stats, Action<IEnumerable> consume)
        {
            // the documents are read again on every round, the map enumerator disposes them
            var items = database.DocumentsStorage.GetDocumentsFrom(queryContext.Documents, Collection, 0, 0, long.MaxValue)
                .Select(document => (IndexItem)new DocumentIndexItem(document.Id, document.LowerId, document.Etag, document.LastModified, document.Data.Size, document, document.Flags));

            using (var enumerator = index.GetMapEnumerator(items, Collection, indexContext, stats, type))
            {
                while (enumerator.MoveNext(queryContext.Documents, out IEnumerable mapResults, out _))
                    consume(mapResults);
            }
        }

        private static async Task StoreOrders(IDocumentStore store)
        {
            using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < NumberOfOrders; i++)
                {
                    var lines = new List<OrderLine>();
                    for (int j = 0; j < LinesPerOrder; j++)
                    {
                        lines.Add(new OrderLine
                        {
                            Product = $"products/{(i * LinesPerOrder + j) % 77 + 1}-A",
                            PricePerUnit = 10.5m + j,
                            Quantity = 1 + (i + j) % 20,
                            Discount = j * 0.05
                        });
                    }

                    await bulkInsert.StoreAsync(new Order
                    {
                        Employee = $"employees/{i % 9 + 1}-A",
                        OrderedAt = new DateTime(2025, 1, 1).AddDays(i % 365),
                        Lines = lines
                    }, $"orders/{i + 1}-A");
                }
            }
        }

        private class Order
        {
            public string Employee { get; set; }
            public DateTime OrderedAt { get; set; }
            public List<OrderLine> Lines { get; set; }
        }

        private class OrderLine
        {
            public string Product { get; set; }
            public decimal PricePerUnit { get; set; }
            public int Quantity { get; set; }
            public double Discount { get; set; }
        }
    }
}
