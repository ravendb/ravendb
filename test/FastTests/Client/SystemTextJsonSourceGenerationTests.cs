using System;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Json.Serialization.SystemTextJson;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client
{
    public partial class SystemTextJsonSourceGenerationTests : RavenTestBase
    {
        public SystemTextJsonSourceGenerationTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CanStoreAndLoadWithSourceGenContext()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions
                    {
                        SourceGenerationContext = TestJsonContext.Default
                    };
                }
            });

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order
                {
                    Id = "orders/1",
                    CustomerId = "customers/1",
                    Total = 150.50m,
                    Lines = new List<OrderLine>
                    {
                        new OrderLine { ProductName = "Widget", Quantity = 3, Price = 25.00m },
                        new OrderLine { ProductName = "Gadget", Quantity = 1, Price = 75.50m }
                    }
                });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<Order>("orders/1");
                Assert.NotNull(order);
                Assert.Equal("customers/1", order.CustomerId);
                Assert.Equal(150.50m, order.Total);
                Assert.Equal(2, order.Lines.Count);
                Assert.Equal("Widget", order.Lines[0].ProductName);
                Assert.Equal(3, order.Lines[0].Quantity);
                Assert.Equal(25.00m, order.Lines[0].Price);
                Assert.Equal("Gadget", order.Lines[1].ProductName);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CanQueryWithSourceGenContext()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions
                    {
                        SourceGenerationContext = TestJsonContext.Default
                    };
                }
            });

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order { Id = "orders/1", CustomerId = "customers/1", Total = 100m, Lines = new List<OrderLine>() });
                await session.StoreAsync(new Order { Id = "orders/2", CustomerId = "customers/2", Total = 200m, Lines = new List<OrderLine>() });
                await session.StoreAsync(new Order { Id = "orders/3", CustomerId = "customers/1", Total = 300m, Lines = new List<OrderLine>() });
                await session.SaveChangesAsync();
            }

            Indexes.WaitForIndexing(store);

            using (var session = store.OpenAsyncSession())
            {
                var orders = await session.Query<Order>()
                    .Where(o => o.CustomerId == "customers/1")
                    .OrderBy(o => o.Total)
                    .ToListAsync();

                Assert.Equal(2, orders.Count);
                Assert.Equal(100m, orders[0].Total);
                Assert.Equal(300m, orders[1].Total);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CanModifyAndSaveChangesWithSourceGenContext()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions
                    {
                        SourceGenerationContext = TestJsonContext.Default
                    };
                }
            });

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new Order
                {
                    Id = "orders/1",
                    CustomerId = "customers/1",
                    Total = 50m,
                    Lines = new List<OrderLine>()
                });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<Order>("orders/1");
                order.Total = 999m;
                order.Lines.Add(new OrderLine { ProductName = "New Item", Quantity = 5, Price = 10m });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<Order>("orders/1");
                Assert.Equal(999m, order.Total);
                Assert.Single(order.Lines);
                Assert.Equal("New Item", order.Lines[0].ProductName);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CanBulkInsertWithSourceGenContext()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions
                    {
                        SourceGenerationContext = TestJsonContext.Default
                    };
                }
            });

            await using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < 50; i++)
                {
                    await bulkInsert.StoreAsync(new Order
                    {
                        Id = $"orders/{i}",
                        CustomerId = $"customers/{i % 5}",
                        Total = i * 10m,
                        Lines = new List<OrderLine>
                        {
                            new OrderLine { ProductName = $"Product {i}", Quantity = i + 1, Price = i * 2.5m }
                        }
                    });
                }
            }

            using (var session = store.OpenAsyncSession())
            {
                var order0 = await session.LoadAsync<Order>("orders/0");
                Assert.NotNull(order0);
                Assert.Equal("customers/0", order0.CustomerId);
                Assert.Equal(0m, order0.Total);

                var order49 = await session.LoadAsync<Order>("orders/49");
                Assert.NotNull(order49);
                Assert.Equal("customers/4", order49.CustomerId);
                Assert.Equal(490m, order49.Total);
                Assert.Equal("Product 49", order49.Lines[0].ProductName);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task TypesNotInContextFallBackToReflection()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions
                    {
                        SourceGenerationContext = TestJsonContext.Default
                    };
                }
            });

            // FallbackEntity is NOT listed in the JsonSerializerContext,
            // so it should fall back to reflection-based serialization
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new FallbackEntity
                {
                    Id = "fallback/1",
                    Value = "reflection works"
                });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var loaded = await session.LoadAsync<FallbackEntity>("fallback/1");
                Assert.NotNull(loaded);
                Assert.Equal("reflection works", loaded.Value);
            }
        }

        // --- Entity classes ---

        public class Order
        {
            public string Id { get; set; }
            public string CustomerId { get; set; }
            public decimal Total { get; set; }
            public List<OrderLine> Lines { get; set; }
        }

        public class OrderLine
        {
            public string ProductName { get; set; }
            public int Quantity { get; set; }
            public decimal Price { get; set; }
        }

        // This type is intentionally NOT included in the source-gen context
        public class FallbackEntity
        {
            public string Id { get; set; }
            public string Value { get; set; }
        }

        // --- Source-generated context ---
        // The [JsonSerializable] attributes tell the compiler to generate
        // serialization metadata for these types at build time, avoiding
        // runtime reflection.

        [JsonSerializable(typeof(Order))]
        [JsonSerializable(typeof(OrderLine))]
        [JsonSerializable(typeof(List<OrderLine>))]
        internal partial class TestJsonContext : JsonSerializerContext
        {
        }
    }
}
