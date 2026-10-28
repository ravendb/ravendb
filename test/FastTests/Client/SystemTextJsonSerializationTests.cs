using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Microsoft.AspNetCore.JsonPatch;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Raven.Client.Json.Serialization.SystemTextJson;
using Raven.Client.ServerWide;
using Sparrow.Json;
using Sparrow.Json.Parsing;
using Sparrow.Json.Sync;
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
        public async Task CanStoreAndLoadPublicFieldsWithSystemTextJson()
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
                await session.StoreAsync(new FieldsEntity("hidden") { Id = "fields/1", Name = "Oren", Count = 3 });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var raw = await session.LoadAsync<BlittableJsonReaderObject>("fields/1");
                Assert.True(raw.TryGet(nameof(FieldsEntity.Name), out string name));
                Assert.Equal("Oren", name);
                Assert.False(raw.TryGet("_secret", out object _));
            }

            using (var session = store.OpenAsyncSession())
            {
                var loaded = await session.LoadAsync<FieldsEntity>("fields/1");
                Assert.Equal("Oren", loaded.Name);
                Assert.Equal(3, loaded.Count);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void DatabaseRecordFieldsAreSerializedWithSystemTextJson()
        {
            var conventions = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (BlittableJsonReaderObject json = conventions.Serialization.DefaultConverter.ToBlittable(new DatabaseRecord("db1"), context))
            {
                Assert.True(json.TryGet(nameof(DatabaseRecord.DatabaseName), out string databaseName));
                Assert.Equal("db1", databaseName);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CanStoreAndLoadStringsThatNeedEscapingWithSystemTextJson()
        {
            string text = "say \"hi\" \\ path" + (char)1 + " " + (char)0x17C + " <tag> & 'q'";

            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions();
                }
            });

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new User { Id = "users/1", Name = text });
                await session.SaveChangesAsync();
            }

            await using (var bulkInsert = store.BulkInsert())
            {
                await bulkInsert.StoreAsync(new User { Id = "users/2", Name = text });
            }

            using (var session = store.OpenAsyncSession())
            {
                Assert.Equal(text, (await session.LoadAsync<User>("users/1")).Name);
                Assert.Equal(text, (await session.LoadAsync<User>("users/2")).Name);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void FloatingPointNumbersAreWrittenLikeNewtonsoft()
        {
            var numbers = new FloatingPointNumbers
            {
                Integral = 2,
                Fraction = -0.5,
                Huge = 1e300,
                Max = double.MaxValue,
                Epsilon = double.Epsilon,
                NaN = double.NaN,
                PositiveInfinity = double.PositiveInfinity,
                NegativeInfinity = double.NegativeInfinity,
                Single = 0.1f,
                SingleMax = float.MaxValue,
                IntegralDecimal = 3m,
                ScaledDecimal = 2.50m,
                MaxDecimal = decimal.MaxValue,
                NullableIntegral = 4
            };

            var newtonsoft = new DocumentConventions { Serialization = new NewtonsoftJsonSerializationConventions() };
            var systemTextJson = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (BlittableJsonReaderObject expected = newtonsoft.Serialization.DefaultConverter.ToBlittable(numbers, context))
            using (BlittableJsonReaderObject actual = systemTextJson.Serialization.DefaultConverter.ToBlittable(numbers, context))
            {
                foreach (string property in expected.GetPropertyNames())
                {
                    Assert.True(expected.TryGetMember(property, out object expectedValue));
                    Assert.True(actual.TryGetMember(property, out object actualValue), property);
                    Assert.IsType<LazyNumberValue>(actualValue);
                    Assert.Equal(expectedValue.ToString(), actualValue.ToString());
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void IntegralNumbersCanBeReadFromDecimalJsonNumbers()
        {
            var conventions = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (BlittableJsonReaderObject json = context.Sync.ReadForMemory(
                       "{\"Int\":3.0,\"Long\":4.0,\"Short\":5.0,\"Byte\":6.0,\"SByte\":-7.0,\"UInt\":8.0,\"ULong\":9.0,\"UShort\":10.0,\"NullableInt\":11.0,\"Truncated\":12.9,\"FromString\":\"13\"," +
                       "\"IntKeys\":{\"1\":2},\"DoubleKeys\":{\"1.5\":3}}", "numbers"))
            {
                var numbers = conventions.Serialization.DefaultConverter.FromBlittable<IntegralNumbers>(json);

                Assert.Equal(3, numbers.Int);
                Assert.Equal(4L, numbers.Long);
                Assert.Equal((short)5, numbers.Short);
                Assert.Equal((byte)6, numbers.Byte);
                Assert.Equal((sbyte)-7, numbers.SByte);
                Assert.Equal(8u, numbers.UInt);
                Assert.Equal(9ul, numbers.ULong);
                Assert.Equal((ushort)10, numbers.UShort);
                Assert.Equal(11, numbers.NullableInt);
                Assert.Equal(12, numbers.Truncated);
                Assert.Equal(13, numbers.FromString);
                Assert.Equal(2, numbers.IntKeys[1]);
                Assert.Equal(3, numbers.DoubleKeys[1.5]);

                using (BlittableJsonReaderObject written = conventions.Serialization.DefaultConverter.ToBlittable(numbers, context))
                {
                    Assert.True(written.TryGet(nameof(IntegralNumbers.IntKeys), out BlittableJsonReaderObject intKeys));
                    Assert.True(intKeys.TryGet("1", out long intKeyValue));
                    Assert.Equal(2, intKeyValue);
                    Assert.True(written.TryGet(nameof(IntegralNumbers.DoubleKeys), out BlittableJsonReaderObject doubleKeys));
                    Assert.True(doubleKeys.TryGet("1.5", out long doubleKeyValue));
                    Assert.Equal(3, doubleKeyValue);
                }
            }
        }

        private class IntegralNumbers
        {
            public int Int { get; set; }
            public long Long { get; set; }
            public short Short { get; set; }
            public byte Byte { get; set; }
            public sbyte SByte { get; set; }
            public uint UInt { get; set; }
            public ulong ULong { get; set; }
            public ushort UShort { get; set; }
            public int? NullableInt { get; set; }
            public int Truncated { get; set; }
            public int FromString { get; set; }
            public Dictionary<int, int> IntKeys { get; set; }
            public Dictionary<double, int> DoubleKeys { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void CustomEnumerableWithEnumerableConstructorRoundTrips()
        {
            var conventions = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };
            var entity = new WithCustomEnumerable { Names = new NamesEnumerable(["a", "b"]) };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (BlittableJsonReaderObject json = conventions.Serialization.DefaultConverter.ToBlittable(entity, context))
            {
                Assert.True(json.TryGet(nameof(WithCustomEnumerable.Names), out BlittableJsonReaderArray names));
                Assert.Equal(2, names.Length);

                var read = conventions.Serialization.DefaultConverter.FromBlittable<WithCustomEnumerable>(json);
                Assert.Equal(["a", "b"], read.Names);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void DocumentModificationsAreAppliedWhenDeserializing()
        {
            var conventions = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (BlittableJsonReaderObject json = context.Sync.ReadForMemory("{\"Name\":\"a\",\"Age\":1}", "modified"))
            {
                json.Modifications = new DynamicJsonValue(json) { [nameof(ModifiedUser.Age)] = 5 };
                json.Modifications.Remove(nameof(ModifiedUser.Name));

                var user = conventions.Serialization.DefaultConverter.FromBlittable<ModifiedUser>(json);

                Assert.Null(user.Name);
                Assert.Equal(5, user.Age);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void NamedFloatingPointLiteralsAreReadBack()
        {
            var conventions = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };
            var literals = new NamedLiterals
            {
                NaN = double.NaN,
                PositiveInfinity = double.PositiveInfinity,
                NegativeInfinity = float.NegativeInfinity,
                Text = "NaN \"Infinity\" -Infinity"
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (BlittableJsonReaderObject json = conventions.Serialization.DefaultConverter.ToBlittable(literals, context))
            {
                var read = conventions.Serialization.DefaultConverter.FromBlittable<NamedLiterals>(json);

                Assert.True(double.IsNaN(read.NaN));
                Assert.Equal(double.PositiveInfinity, read.PositiveInfinity);
                Assert.Equal(float.NegativeInfinity, read.NegativeInfinity);
                Assert.Equal(literals.Text, read.Text);
            }
        }

        private class NamedLiterals
        {
            public double NaN { get; set; }

            public double PositiveInfinity { get; set; }

            public float NegativeInfinity { get; set; }

            public string Text { get; set; }
        }

        private class ModifiedUser
        {
            public string Name { get; set; }

            public int Age { get; set; }
        }

        private class NamesEnumerable : IEnumerable<string>
        {
            private readonly List<string> _names;

            public NamesEnumerable(IEnumerable<string> names)
            {
                _names = names.ToList();
            }

            public IEnumerator<string> GetEnumerator() => _names.GetEnumerator();

            IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();
        }

        private class WithCustomEnumerable
        {
            public NamesEnumerable Names { get; set; }
        }

        private class FloatingPointNumbers
        {
            public double Integral { get; set; }
            public double Fraction { get; set; }
            public double Huge { get; set; }
            public double Max { get; set; }
            public double Epsilon { get; set; }
            public double NaN { get; set; }
            public double PositiveInfinity { get; set; }
            public double NegativeInfinity { get; set; }
            public float Single { get; set; }
            public float SingleMax { get; set; }
            public decimal IntegralDecimal { get; set; }
            public decimal ScaledDecimal { get; set; }
            public decimal MaxDecimal { get; set; }
            public double? NullableIntegral { get; set; }
        }

        private class FieldsEntity
        {
            private readonly string _secret;

            public FieldsEntity()
            {
            }

            public FieldsEntity(string secret)
            {
                _secret = secret;
            }

            public string Id;
            public string Name;
            public int Count;

            public string GetSecret() => _secret;
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
                Assert.Equal(new DateTimeOffset(2025, 6, 15, 10, 30, 0, TimeSpan.FromHours(2)), loaded.DateTimeOffsetProp);
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

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CanLoadDocumentWhoseJsonIsMuchLargerThanItsBlittable()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions();
                }
            });

            var shipment = new Shipment
            {
                Id = "shipments/1",
                Items = Enumerable.Range(0, 500)
                    .Select(i => new ShipmentItem { CustomerReferenceNumber = i, WarehouseLocationCode = i * 2, IsAwaitingBackorder = i % 2 == 0 })
                    .ToList()
            };

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(shipment);
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var loaded = await session.LoadAsync<Shipment>("shipments/1");
                Assert.NotNull(loaded);
                Assert.Equal(500, loaded.Items.Count);
                Assert.Equal(499, loaded.Items[499].CustomerReferenceNumber);
                Assert.Equal(998, loaded.Items[499].WarehouseLocationCode);
                Assert.False(loaded.Items[499].IsAwaitingBackorder);
            }
        }

        private class Shipment
        {
            public string Id { get; set; }
            public List<ShipmentItem> Items { get; set; }
        }

        private class ShipmentItem
        {
            public int CustomerReferenceNumber { get; set; }
            public int WarehouseLocationCode { get; set; }
            public bool IsAwaitingBackorder { get; set; }
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

        // Entity that only has Name — "ExtraField" and "ExtraNumber" are not on this type
        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task SwitchingToSystemTextJsonKeepsNonPublicMembersOfStoredDocuments()
        {
            using var store = GetDocumentStore();

            using (var session = store.OpenAsyncSession())
            {
                var order = new EncapsulatedOrder("Shipped");
                order.AddLine("line/1");
                await session.StoreAsync(order, "orders/1");
                await session.SaveChangesAsync();
            }

            using (var systemTextJsonStore = new DocumentStore { Urls = store.Urls, Database = store.Database })
            {
                systemTextJsonStore.Conventions.Serialization = new SystemTextJsonSerializationConventions();
                systemTextJsonStore.Initialize();

                using (var session = systemTextJsonStore.OpenAsyncSession())
                {
                    var order = await session.LoadAsync<EncapsulatedOrder>("orders/1");
                    Assert.Equal("Shipped", order.Status);
                    Assert.Equal(["line/1"], order.Lines);
                    Assert.False(session.Advanced.HasChanges);
                }
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public async Task PatchOperationSerializesValuesWithStoreConventions()
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
                await session.StoreAsync(new User { Id = "users/1", Name = "Oren" });
                await session.SaveChangesAsync();
            }

            await store.Operations.SendAsync(new PatchOperation("users/1", null, new PatchRequest
            {
                Script = "this.Payload = args.payload",
                Values = { ["payload"] = new RenamedPayload { Name = "x" } }
            }));

            using (var session = store.OpenAsyncSession())
            {
                var raw = await session.LoadAsync<BlittableJsonReaderObject>("users/1");
                Assert.True(raw.TryGet("Payload", out BlittableJsonReaderObject payload));
                Assert.Equal(["n"], payload.GetPropertyNames());
                Assert.Equal("x", payload["n"].ToString());
            }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public async Task SessionJsonPatchSerializesValuesWithStoreConventions()
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
                await session.StoreAsync(new User { Id = "users/1", Name = "Oren" });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var patch = new JsonPatchDocument();
                patch.Add("/Payload", new RenamedPayload { Name = "x" });
                session.Advanced.Defer(new JsonPatchCommandData("users/1", patch));
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var raw = await session.LoadAsync<BlittableJsonReaderObject>("users/1");
                Assert.True(raw.TryGet("Payload", out BlittableJsonReaderObject payload));
                Assert.Equal(["n"], payload.GetPropertyNames());
                Assert.Equal("x", payload["n"].ToString());
            }
        }

        private class RenamedPayload
        {
            [JsonPropertyName("n")]
            public string Name { get; set; }
        }

        private class EncapsulatedOrder
        {
            private readonly List<string> _lines = [];

            public EncapsulatedOrder(string status)
            {
                Status = status;
            }

            private EncapsulatedOrder()
            {
            }

            public string Id { get; private set; }

            public string Status { get; private set; }

            public IReadOnlyList<string> Lines => _lines;

            public void AddLine(string line) => _lines.Add(line);
        }

        private class PartialEntity
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task PreserveDocumentPropertiesNotFoundOnModel_RoundTrips()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions();
                }
            });

            // Store a document with extra properties using an anonymous type
            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new
                {
                    Name = "John",
                    ExtraField = "should be preserved",
                    ExtraNumber = 42
                }, "items/1");
                await session.SaveChangesAsync();
            }

            // Load as PartialEntity (missing ExtraField and ExtraNumber), modify, save
            using (var session = store.OpenAsyncSession())
            {
                var entity = await session.LoadAsync<PartialEntity>("items/1");
                Assert.Equal("John", entity.Name);

                entity.Name = "Jane";
                await session.SaveChangesAsync();
            }

            // Verify extra properties survived the round-trip by loading as dynamic
            using (var session = store.OpenAsyncSession())
            {
                var raw = await session.LoadAsync<BlittableJsonReaderObject>("items/1");
                Assert.True(raw.TryGet("Name", out string name));
                Assert.Equal("Jane", name);
                Assert.True(raw.TryGet("ExtraField", out string extraField));
                Assert.Equal("should be preserved", extraField);
                Assert.True(raw.TryGet("ExtraNumber", out long extraNumber));
                Assert.Equal(42, extraNumber);
            }
        }
    }
}
