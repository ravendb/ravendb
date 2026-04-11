using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Session;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Client.Json.Serialization.SystemTextJson;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client
{
    public class SystemTextJsonEndToEndBenchmarks : RavenTestBase
    {
        public SystemTextJsonEndToEndBenchmarks(Xunit.ITestOutputHelper output) : base(output)
        {
        }

        private const int WarmupIterations = 5;
        private const int MeasuredIterations = 50;
        private const int BulkInsertCount = 1000;
        private const int QueryDocCount = 200;

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task BenchmarkStoreAndLoad()
        {
            var results = new List<string> { "", "=== Store & Load (single document, " + MeasuredIterations + " iterations) ===" };

            foreach (var (name, conventions) in GetSerializers())
            {
                using var store = GetDocumentStore(new Options
                {
                    ModifyDocumentStore = s => s.Conventions.Serialization = conventions
                });

                // Warmup
                for (int i = 0; i < WarmupIterations; i++)
                {
                    using var session = store.OpenAsyncSession();
                    await session.StoreAsync(CreateMediumEntity(i), $"warmup/{i}");
                    await session.SaveChangesAsync();
                }

                // Measure Store
                var storeSw = Stopwatch.StartNew();
                for (int i = 0; i < MeasuredIterations; i++)
                {
                    using var session = store.OpenAsyncSession();
                    await session.StoreAsync(CreateMediumEntity(i), $"bench/{i}");
                    await session.SaveChangesAsync();
                }
                storeSw.Stop();

                // Measure Load
                var loadSw = Stopwatch.StartNew();
                for (int i = 0; i < MeasuredIterations; i++)
                {
                    using var session = store.OpenAsyncSession();
                    var entity = await session.LoadAsync<Order>($"bench/{i}");
                    GC.KeepAlive(entity);
                }
                loadSw.Stop();

                results.Add($"  {name,-12} Store: {storeSw.ElapsedMilliseconds,6}ms ({storeSw.ElapsedMilliseconds * 1000.0 / MeasuredIterations:F1} us/op)  " +
                            $"Load: {loadSw.ElapsedMilliseconds,6}ms ({loadSw.ElapsedMilliseconds * 1000.0 / MeasuredIterations:F1} us/op)");
            }

            foreach (var line in results)
                Output.WriteLine(line);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task BenchmarkBulkInsert()
        {
            var results = new List<string> { "", $"=== Bulk Insert ({BulkInsertCount} documents) ===" };

            foreach (var (name, conventions) in GetSerializers())
            {
                using var store = GetDocumentStore(new Options
                {
                    ModifyDocumentStore = s => s.Conventions.Serialization = conventions
                });

                // Warmup
                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < 50; i++)
                        await bulk.StoreAsync(CreateMediumEntity(i));
                }

                // Measure
                var sw = Stopwatch.StartNew();
                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < BulkInsertCount; i++)
                        await bulk.StoreAsync(CreateMediumEntity(i));
                }
                sw.Stop();

                // Verify
                using (var session = store.OpenAsyncSession())
                {
                    var count = await session.Query<Order>().CountAsync();
                    Assert.True(count > 0);
                }

                results.Add($"  {name,-12} {sw.ElapsedMilliseconds,6}ms ({sw.ElapsedMilliseconds * 1000.0 / BulkInsertCount:F1} us/doc)");
            }

            foreach (var line in results)
                Output.WriteLine(line);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task BenchmarkQuery()
        {
            var results = new List<string> { "", $"=== Query ({QueryDocCount} docs, {MeasuredIterations} queries) ===" };

            foreach (var (name, conventions) in GetSerializers())
            {
                using var store = GetDocumentStore(new Options
                {
                    ModifyDocumentStore = s => s.Conventions.Serialization = conventions
                });

                // Seed data
                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < QueryDocCount; i++)
                        await bulk.StoreAsync(CreateMediumEntity(i));
                }

                Indexes.WaitForIndexing(store);

                // Warmup
                for (int i = 0; i < WarmupIterations; i++)
                {
                    using var session = store.OpenAsyncSession();
                    var _ = await session.Query<Order>()
                        .Where(x => x.Total > 500)
                        .ToListAsync();
                }

                // Measure
                var sw = Stopwatch.StartNew();
                for (int i = 0; i < MeasuredIterations; i++)
                {
                    using var session = store.OpenAsyncSession();
                    var result = await session.Query<Order>()
                        .Where(x => x.Total > 500)
                        .Take(50)
                        .ToListAsync();
                    GC.KeepAlive(result);
                }
                sw.Stop();

                results.Add($"  {name,-12} {sw.ElapsedMilliseconds,6}ms ({sw.ElapsedMilliseconds * 1000.0 / MeasuredIterations:F1} us/query)");
            }

            foreach (var line in results)
                Output.WriteLine(line);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task BenchmarkStreaming()
        {
            var results = new List<string> { "", $"=== Stream ({QueryDocCount} docs, {MeasuredIterations / 5} streams) ===" };
            int streamIterations = MeasuredIterations / 5;

            foreach (var (name, conventions) in GetSerializers())
            {
                using var store = GetDocumentStore(new Options
                {
                    ModifyDocumentStore = s => s.Conventions.Serialization = conventions
                });

                // Seed data
                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < QueryDocCount; i++)
                        await bulk.StoreAsync(CreateMediumEntity(i));
                }

                Indexes.WaitForIndexing(store);

                // Warmup
                for (int i = 0; i < 2; i++)
                {
                    using var session = store.OpenAsyncSession();
                    var stream = await session.Advanced.StreamAsync<Order>("orders/");
                    while (await stream.MoveNextAsync())
                        GC.KeepAlive(stream.Current.Document);
                }

                // Measure
                var sw = Stopwatch.StartNew();
                int totalDocs = 0;
                for (int i = 0; i < streamIterations; i++)
                {
                    using var session = store.OpenAsyncSession();
                    var stream = await session.Advanced.StreamAsync<Order>("orders/");
                    while (await stream.MoveNextAsync())
                    {
                        GC.KeepAlive(stream.Current.Document);
                        totalDocs++;
                    }
                }
                sw.Stop();

                results.Add($"  {name,-12} {sw.ElapsedMilliseconds,6}ms ({totalDocs} docs, {sw.ElapsedMilliseconds * 1000.0 / totalDocs:F1} us/doc)");
            }

            foreach (var line in results)
                Output.WriteLine(line);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task BenchmarkModifyAndSave()
        {
            var results = new List<string> { "", $"=== Load-Modify-Save ({MeasuredIterations} iterations) ===" };

            foreach (var (name, conventions) in GetSerializers())
            {
                using var store = GetDocumentStore(new Options
                {
                    ModifyDocumentStore = s => s.Conventions.Serialization = conventions
                });

                // Seed data
                using (var bulk = store.BulkInsert())
                {
                    for (int i = 0; i < MeasuredIterations; i++)
                        await bulk.StoreAsync(CreateMediumEntity(i), $"orders/{i}");
                }

                // Warmup
                for (int i = 0; i < WarmupIterations; i++)
                {
                    using var session = store.OpenAsyncSession();
                    var entity = await session.LoadAsync<Order>($"orders/{i}");
                    entity.Total += 1;
                    await session.SaveChangesAsync();
                }

                // Measure
                var sw = Stopwatch.StartNew();
                for (int i = 0; i < MeasuredIterations; i++)
                {
                    using var session = store.OpenAsyncSession();
                    var entity = await session.LoadAsync<Order>($"orders/{i}");
                    entity.Total += 100;
                    entity.Notes = $"Updated at iteration {i}";
                    await session.SaveChangesAsync();
                }
                sw.Stop();

                results.Add($"  {name,-12} {sw.ElapsedMilliseconds,6}ms ({sw.ElapsedMilliseconds * 1000.0 / MeasuredIterations:F1} us/op)");
            }

            foreach (var line in results)
                Output.WriteLine(line);
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task BenchmarkLargeDocuments()
        {
            int count = 20;
            var results = new List<string> { "", $"=== Large Documents (100-item nested, {count} docs) ===" };

            foreach (var (name, conventions) in GetSerializers())
            {
                using var store = GetDocumentStore(new Options
                {
                    ModifyDocumentStore = s => s.Conventions.Serialization = conventions
                });

                var docs = Enumerable.Range(0, count).Select(CreateLargeEntity).ToList();

                // Measure Store
                var storeSw = Stopwatch.StartNew();
                for (int i = 0; i < count; i++)
                {
                    using var session = store.OpenAsyncSession();
                    await session.StoreAsync(docs[i], $"reports/{i}");
                    await session.SaveChangesAsync();
                }
                storeSw.Stop();

                // Measure Load
                var loadSw = Stopwatch.StartNew();
                for (int i = 0; i < count; i++)
                {
                    using var session = store.OpenAsyncSession();
                    var entity = await session.LoadAsync<Report>($"reports/{i}");
                    GC.KeepAlive(entity);
                }
                loadSw.Stop();

                results.Add($"  {name,-12} Store: {storeSw.ElapsedMilliseconds,6}ms ({storeSw.ElapsedMilliseconds * 1000.0 / count:F0} us/doc)  " +
                            $"Load: {loadSw.ElapsedMilliseconds,6}ms ({loadSw.ElapsedMilliseconds * 1000.0 / count:F0} us/doc)");
            }

            foreach (var line in results)
                Output.WriteLine(line);
        }

        // === Helpers ===

        private static List<(string Name, Raven.Client.Json.Serialization.ISerializationConventions Conventions)> GetSerializers()
        {
            return new List<(string, Raven.Client.Json.Serialization.ISerializationConventions)>
            {
                ("Newtonsoft", new NewtonsoftJsonSerializationConventions()),
                ("STJ", new SystemTextJsonSerializationConventions())
            };
        }

        private static Order CreateMediumEntity(int i)
        {
            return new Order
            {
                Id = $"orders/{i}",
                CustomerName = $"Customer {i}",
                Email = $"customer{i}@example.com",
                OrderDate = new DateTime(2026, 1, 1, 0, 0, 0, DateTimeKind.Utc).AddDays(i),
                Total = 100m + i * 13.7m,
                Tax = 10m + i * 1.37m,
                IsShipped = i % 3 == 0,
                Notes = $"Order notes for item {i}",
                ShippingAddress = new Address
                {
                    Street = $"{i} Main St",
                    City = "Springfield",
                    State = "IL",
                    ZipCode = "62701"
                },
                Lines = new List<OrderLine>
                {
                    new OrderLine { ProductName = $"Product A-{i}", Quantity = i + 1, Price = 29.99m },
                    new OrderLine { ProductName = $"Product B-{i}", Quantity = 2, Price = 49.99m }
                },
                Tags = new List<string> { "tag1", "tag2", $"tag{i}" }
            };
        }

        private static Report CreateLargeEntity(int i)
        {
            var report = new Report
            {
                Id = $"reports/{i}",
                Title = $"Report {i}",
                GeneratedAt = new DateTime(2026, 4, 11, 12, 0, 0, DateTimeKind.Utc),
                Items = new List<ReportItem>()
            };
            for (int j = 0; j < 100; j++)
            {
                report.Items.Add(new ReportItem
                {
                    ProductName = $"Product {j}",
                    Quantity = j * 10,
                    UnitPrice = 9.99m + j,
                    TotalPrice = (9.99m + j) * (j * 10),
                    Category = j % 5 == 0 ? "Electronics" : j % 3 == 0 ? "Clothing" : "Food"
                });
            }
            return report;
        }

        // === Entity Classes ===

        private class Order
        {
            public string Id { get; set; }
            public string CustomerName { get; set; }
            public string Email { get; set; }
            public DateTime OrderDate { get; set; }
            public decimal Total { get; set; }
            public decimal Tax { get; set; }
            public bool IsShipped { get; set; }
            public string Notes { get; set; }
            public Address ShippingAddress { get; set; }
            public List<OrderLine> Lines { get; set; }
            public List<string> Tags { get; set; }
        }

        private class Address
        {
            public string Street { get; set; }
            public string City { get; set; }
            public string State { get; set; }
            public string ZipCode { get; set; }
        }

        private class OrderLine
        {
            public string ProductName { get; set; }
            public int Quantity { get; set; }
            public decimal Price { get; set; }
        }

        private class Report
        {
            public string Id { get; set; }
            public string Title { get; set; }
            public DateTime GeneratedAt { get; set; }
            public List<ReportItem> Items { get; set; }
        }

        private class ReportItem
        {
            public string ProductName { get; set; }
            public int Quantity { get; set; }
            public decimal UnitPrice { get; set; }
            public decimal TotalPrice { get; set; }
            public string Category { get; set; }
        }
    }
}
