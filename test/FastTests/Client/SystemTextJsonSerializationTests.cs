using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents;
using Raven.Client.Json.Serialization.SystemTextJson;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Client
{
    public class SystemTextJsonSerializationTests : RavenTestBase
    {
        public SystemTextJsonSerializationTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CanStoreAndLoadWithSystemTextJson()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions();
                }
            });

            var now = DateTime.UtcNow;

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Id = "users/1",
                    Name = "Oren",
                    Age = 42,
                    IsActive = true,
                    CreatedAt = now
                });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var loaded = await session.LoadAsync<User>("users/1");
                Assert.NotNull(loaded);
                Assert.Equal("Oren", loaded.Name);
                Assert.Equal(42, loaded.Age);
                Assert.True(loaded.IsActive);
                Assert.Equal(now.Date, loaded.CreatedAt.Date);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CanHandleMultiplePropertyTypes()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions();
                }
            });

            var entity = new AllTypesEntity
            {
                Id = "alltypes/1",
                StringProp = "hello",
                IntProp = 42,
                LongProp = 9_999_999_999L,
                DoubleProp = 3.14,
                DecimalProp = 99.99m,
                BoolProp = true,
                DateTimeProp = new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Utc),
                DateTimeOffsetProp = new DateTimeOffset(2025, 6, 15, 10, 30, 0, TimeSpan.FromHours(2)),
                GuidProp = Guid.Parse("12345678-1234-1234-1234-123456789abc"),
                TimeSpanProp = TimeSpan.FromHours(2.5),
                NullableIntProp = 7,
                NullableIntNull = null,
                NullableDateTimeProp = null,
                NullString = null
            };

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var loaded = await session.LoadAsync<AllTypesEntity>("alltypes/1");
                Assert.NotNull(loaded);
                Assert.Equal("hello", loaded.StringProp);
                Assert.Equal(42, loaded.IntProp);
                Assert.Equal(9_999_999_999L, loaded.LongProp);
                Assert.Equal(3.14, loaded.DoubleProp);
                Assert.Equal(99.99m, loaded.DecimalProp);
                Assert.True(loaded.BoolProp);
                Assert.Equal(new DateTime(2025, 6, 15, 10, 30, 0, DateTimeKind.Utc), loaded.DateTimeProp);
                Assert.Equal(Guid.Parse("12345678-1234-1234-1234-123456789abc"), loaded.GuidProp);
                Assert.Equal(TimeSpan.FromHours(2.5), loaded.TimeSpanProp);
                Assert.Equal(7, loaded.NullableIntProp);
                Assert.Null(loaded.NullableIntNull);
                Assert.Null(loaded.NullableDateTimeProp);
                Assert.Null(loaded.NullString);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CanHandleNestedObjects()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions();
                }
            });

            var order = new Order
            {
                Id = "orders/1",
                CustomerId = "customers/1",
                Total = 150.50m,
                Lines = new List<OrderLine>
                {
                    new OrderLine { ProductName = "Widget", Quantity = 3, Price = 25.00m },
                    new OrderLine { ProductName = "Gadget", Quantity = 1, Price = 75.50m }
                }
            };

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(order);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var loaded = await session.LoadAsync<Order>("orders/1");
                Assert.NotNull(loaded);
                Assert.Equal("customers/1", loaded.CustomerId);
                Assert.Equal(150.50m, loaded.Total);
                Assert.Equal(2, loaded.Lines.Count);
                Assert.Equal("Widget", loaded.Lines[0].ProductName);
                Assert.Equal(3, loaded.Lines[0].Quantity);
                Assert.Equal(25.00m, loaded.Lines[0].Price);
                Assert.Equal("Gadget", loaded.Lines[1].ProductName);
                Assert.Equal(1, loaded.Lines[1].Quantity);
                Assert.Equal(75.50m, loaded.Lines[1].Price);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CanHandleCollections()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions();
                }
            });

            var entity = new CollectionEntity
            {
                Id = "collections/1",
                Tags = new List<string> { "alpha", "beta", "gamma" },
                Scores = new List<int> { 10, 20, 30 },
                Metadata = new Dictionary<string, string>
                {
                    ["key1"] = "value1",
                    ["key2"] = "value2"
                },
                Numbers = new[] { 1, 2, 3, 4, 5 }
            };

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(entity);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var loaded = await session.LoadAsync<CollectionEntity>("collections/1");
                Assert.NotNull(loaded);
                Assert.Equal(new List<string> { "alpha", "beta", "gamma" }, loaded.Tags);
                Assert.Equal(new List<int> { 10, 20, 30 }, loaded.Scores);
                Assert.Equal("value1", loaded.Metadata["key1"]);
                Assert.Equal("value2", loaded.Metadata["key2"]);
                Assert.Equal(new[] { 1, 2, 3, 4, 5 }, loaded.Numbers);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CanQueryWithSystemTextJson()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions();
                }
            });

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Name = "Alice", Age = 30, IsActive = true });
                await session.StoreAsync(new User { Name = "Bob", Age = 25, IsActive = false });
                await session.StoreAsync(new User { Name = "Charlie", Age = 35, IsActive = true });
                await session.SaveChangesAsync();
            }

            Indexes.WaitForIndexing(store);

            using (var session = store.OpenAsyncSession())
            {
                var activeUsers = await session.Query<User>()
                    .Where(u => u.IsActive)
                    .OrderBy(u => u.Name)
                    .ToListAsync();

                Assert.Equal(2, activeUsers.Count);
                Assert.Equal("Alice", activeUsers[0].Name);
                Assert.Equal("Charlie", activeUsers[1].Name);
            }

            using (var session = store.OpenAsyncSession())
            {
                var youngUsers = await session.Query<User>()
                    .Where(u => u.Age < 32)
                    .ToListAsync();

                Assert.Equal(2, youngUsers.Count);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CanModifyAndSaveChangesWithSystemTextJson()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions();
                }
            });

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Id = "users/1",
                    Name = "Original",
                    Age = 20,
                    IsActive = false,
                    CreatedAt = DateTime.UtcNow
                });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1");
                user.Name = "Modified";
                user.Age = 30;
                user.IsActive = true;
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1");
                Assert.Equal("Modified", user.Name);
                Assert.Equal(30, user.Age);
                Assert.True(user.IsActive);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CanBulkInsertWithSystemTextJson()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions();
                }
            });

            const int count = 100;

            await using (var bulkInsert = store.BulkInsert())
            {
                for (int i = 0; i < count; i++)
                {
                    await bulkInsert.StoreAsync(new User
                    {
                        Id = $"users/{i}",
                        Name = $"User {i}",
                        Age = 20 + (i % 50),
                        IsActive = i % 2 == 0,
                        CreatedAt = DateTime.UtcNow
                    });
                }
            }

            using (var session = store.OpenAsyncSession())
            {
                var user0 = await session.LoadAsync<User>("users/0");
                Assert.NotNull(user0);
                Assert.Equal("User 0", user0.Name);
                Assert.Equal(20, user0.Age);
                Assert.True(user0.IsActive);

                var user99 = await session.LoadAsync<User>("users/99");
                Assert.NotNull(user99);
                Assert.Equal("User 99", user99.Name);
                Assert.Equal(69, user99.Age);
                Assert.False(user99.IsActive);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CanDeleteWithSystemTextJson()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions();
                }
            });

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User
                {
                    Id = "users/1",
                    Name = "ToDelete",
                    Age = 25,
                    IsActive = true,
                    CreatedAt = DateTime.UtcNow
                });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1");
                Assert.NotNull(user);
                session.Delete(user);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<User>("users/1");
                Assert.Null(user);
            }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Age { get; set; }
            public bool IsActive { get; set; }
            public DateTime CreatedAt { get; set; }
        }

        private class Order
        {
            public string Id { get; set; }
            public string CustomerId { get; set; }
            public List<OrderLine> Lines { get; set; }
            public decimal Total { get; set; }
        }

        private class OrderLine
        {
            public string ProductName { get; set; }
            public int Quantity { get; set; }
            public decimal Price { get; set; }
        }

        private class AllTypesEntity
        {
            public string Id { get; set; }
            public string StringProp { get; set; }
            public int IntProp { get; set; }
            public long LongProp { get; set; }
            public double DoubleProp { get; set; }
            public decimal DecimalProp { get; set; }
            public bool BoolProp { get; set; }
            public DateTime DateTimeProp { get; set; }
            public DateTimeOffset DateTimeOffsetProp { get; set; }
            public Guid GuidProp { get; set; }
            public TimeSpan TimeSpanProp { get; set; }
            public int? NullableIntProp { get; set; }
            public int? NullableIntNull { get; set; }
            public DateTime? NullableDateTimeProp { get; set; }
            public string NullString { get; set; }
        }

        private class CollectionEntity
        {
            public string Id { get; set; }
            public List<string> Tags { get; set; }
            public List<int> Scores { get; set; }
            public Dictionary<string, string> Metadata { get; set; }
            public int[] Numbers { get; set; }
        }
    }
}
