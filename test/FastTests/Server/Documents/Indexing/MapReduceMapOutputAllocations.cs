using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Server.Documents;
using Raven.Server.Documents.Indexes;
using Raven.Server.Documents.Indexes.MapReduce;
using Raven.Server.Documents.Indexes.MapReduce.Static;
using Raven.Server.Documents.Indexes.Static;
using Raven.Server.ServerWide.Context;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Server.Documents.Indexing
{
    public class MapReduceMapOutputAllocations(ITestOutputHelper output) : RavenTestBase(output)
    {
        private const int NumberOfOrders = 2_000;
        private const int LinesPerOrder = 3;
        private const string Collection = "Orders";
        private const int Rounds = 3;

        // Before RavenDB-27880 every map result created a Newtonsoft BlittableJsonWriter with a new builder, blittable writer and buffers,
        // and every document a new enumerator with its queue, ~2,630 bytes per order with three lines.
        [RavenFact(RavenTestCategory.Indexes)]
        public async Task Map_results_of_order_lines_grouped_by_product()
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

            AssertAtMost(1_300, "Map results of the lines of an order", bytes);
        }

        // Before RavenDB-27880 ~3,770 bytes per order with four results. A multi-map index also looks up the property accessor of every result,
        // which allocated a closure and a delegate.
        [RavenFact(RavenTestCategory.Indexes)]
        public async Task Map_results_of_a_multi_map_index()
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

            AssertAtMost(1_700, "Map results of an order", bytes);
        }

        // Before RavenDB-27880 ~1,120 bytes per order, which included the stack node of the pool that the reduce key of two fields returned its buffer to
        // after every document.
        [RavenFact(RavenTestCategory.Indexes)]
        public async Task Map_results_of_orders_grouped_by_employee_and_month()
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

            AssertAtMost(450, "Map result of an order", bytes);
        }

        // Before RavenDB-27880 ~4,600 bytes per order with three lines. Most of what is left is the enumeration of the properties of the JavaScript objects.
        [RavenFact(RavenTestCategory.Indexes)]
        public async Task Map_results_of_a_javascript_index()
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

            AssertAtMost(3_800, "Map results of the lines of an order", bytes);
        }

        // A map result that cannot be written leaves the builder that the batch reuses in the middle of a document,
        // the documents after it must still be mapped correctly.
        [RavenFact(RavenTestCategory.Indexes)]
        public void Map_results_after_a_failed_one_are_written_correctly()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 30; i++)
                        session.Store(new Item { Name = "item" + i, Category = "c" + i % 2, Value = i }, "items/" + i);
                    session.SaveChanges();
                }

                // a char is not a supported map result value
                store.Maintenance.Send(new PutIndexesOperation(new IndexDefinition
                {
                    Name = "Items/ByCategory",
                    Maps = { @"from item in docs.Items select new { item.Category, Initial = item.Value % 10 == 3 ? (object)((string)item.Name)[0] : item.Name, Count = 1 }" },
                    Reduce = @"from result in results group result by result.Category into g select new { Category = g.Key, Initial = (object)null, Count = g.Sum(x => x.Count) }"
                }));

                Indexes.WaitForIndexing(store, allowErrors: true);

                var errors = store.Maintenance.Send(new GetIndexErrorsOperation(new[] { "Items/ByCategory" }));
                Assert.Equal(3, errors[0].Errors.Length);

                using (var session = store.OpenSession())
                {
                    var results = session.Advanced.RawQuery<CategoryCount>("from index 'Items/ByCategory' order by Category").ToList();

                    Assert.Equal([("c0", 15), ("c1", 12)], results.Select(x => (x.Category, x.Count)));
                }
            }
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

                    var mapFunction = MeasurePerOrder(() => Map(database, index, type, queryContext, indexContext, stats, mapResults =>
                    {
                        foreach (var _ in mapResults)
                            mapped++;
                    }));

                    var bytes = MeasurePerOrder(() => Map(database, index, type, queryContext, indexContext, stats, mapResults =>
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
                    }));

                    Assert.Equal((Rounds + 1L) * NumberOfOrders * resultsPerOrder, mapped);
                    Assert.Equal(mapped, converted);

                    Output.WriteLine($"{"Map function",-48} {mapFunction,8:F1} bytes/order");
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

        private static double MeasurePerOrder(Action batch)
        {
            batch(); // warm up so JIT and pool filling are excluded from the measurement

            var before = GC.GetAllocatedBytesForCurrentThread();
            for (int i = 0; i < Rounds; i++)
                batch();

            return (double)(GC.GetAllocatedBytesForCurrentThread() - before) / (Rounds * NumberOfOrders);
        }

        private void AssertAtMost(int limit, string name, double bytesPerOrder)
        {
            Output.WriteLine($"{name,-48} {bytesPerOrder,8:F1} bytes/order");
#if !DEBUG // debug-only checks allocate too
            Assert.True(bytesPerOrder <= limit, $"{name} allocated {bytesPerOrder:F1} bytes/order, expected at most {limit}");
#endif
        }

        private class Item
        {
            public string Name { get; set; }
            public string Category { get; set; }
            public int Value { get; set; }
        }

        private class CategoryCount
        {
            public string Category { get; set; }
            public int Count { get; set; }
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
