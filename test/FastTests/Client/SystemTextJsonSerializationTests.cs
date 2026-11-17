using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Runtime.Loader;
using System.Text.Json.Serialization;
using System.Threading.Tasks;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Conventions;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries.Facets;
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

        private class WithIgnoredMember
        {
            public string Name { get; set; }

            [JsonIgnore]
            public string Secret { get; set; }
        }

        private class WithSecret
        {
            public string Name { get; set; }

            public string Secret { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void SystemTextJsonIgnoredMembersAreNeitherWrittenNorRead()
        {
            var conventions = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                using (BlittableJsonReaderObject json = conventions.Serialization.DefaultConverter.ToBlittable(new WithIgnoredMember { Name = "a", Secret = "s" }, context))
                    Assert.Equal([nameof(WithIgnoredMember.Name)], json.GetPropertyNames());

                using (BlittableJsonReaderObject json = context.Sync.ReadForMemory("{\"Name\":\"a\",\"Secret\":\"s\"}", "ignored"))
                    Assert.Null(conventions.Serialization.DefaultConverter.FromBlittable<WithIgnoredMember>(json).Secret);
            }
        }

        private sealed class WriterPathChild
        {
            public string Name { get; set; }

            public int Count { get; set; }
        }

        private sealed class WriterPathEntity
        {
            public string Plain { get; set; }

            public string Empty { get; set; }

            public string Escaped { get; set; }

            public string ControlCharacters { get; set; }

            public string Unicode { get; set; }

            public string Long { get; set; }

            public string Null { get; set; }

            public int Number { get; set; }

            public long Big { get; set; }

            public double Fraction { get; set; }

            public decimal Money { get; set; }

            public bool Flag { get; set; }

            public Dictionary<string, string> ByName { get; set; }

            public List<string> Items { get; set; }

            public List<WriterPathChild> Children { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void SystemTextJsonWriterBuildsTheSameDocumentAsTheDefaultConverter()
        {
            var conventions = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };

            var entity = new WriterPathEntity
            {
                Plain = "plain",
                Empty = string.Empty,
                Escaped = "quote \" backslash \\ newline \n tab \t slash / done",
                ControlCharacters = "a\u0001b\u001fc",
                Unicode = "zażółć gęślą jaźń 你好 😀",
                Long = new string('a', 300) + "\"" + new string('b', 300),
                Null = null,
                Number = -42,
                Big = long.MaxValue,
                Fraction = 1.5,
                Money = 12345.6789m,
                Flag = true,
                ByName = new Dictionary<string, string>
                {
                    ["plain"] = "value",
                    ["quoted \" key"] = "quoted \" value",
                    [""] = "empty key",
                    ["zażółć"] = "jaźń",
                    ["line\nbreak"] = null
                },
                Items = ["first", "", "third \\ item"],
                Children =
                [
                    new WriterPathChild { Name = "child \" one", Count = 1 },
                    new WriterPathChild { Name = "child two", Count = 2 }
                ]
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (BlittableJsonReaderObject expected = conventions.Serialization.DefaultConverter.ToBlittable(entity, context))
            using (var writer = conventions.Serialization.CreateWriter(context))
            {
                conventions.Serialization.CreateSerializer().Serialize(writer, entity);
                writer.FinalizeDocument();

                using (BlittableJsonReaderObject actual = writer.CreateReader())
                {
                    Assert.Equal(expected.ToString(), actual.ToString());

                    Assert.True(actual.TryGet(nameof(WriterPathEntity.ByName), out BlittableJsonReaderObject byName));
                    Assert.True(byName.TryGet("quoted \" key", out string quoted));
                    Assert.Equal("quoted \" value", quoted);
                    Assert.True(byName.TryGet("", out string emptyKey));
                    Assert.Equal("empty key", emptyKey);
                    Assert.True(byName.TryGet("zażółć", out string unicode));
                    Assert.Equal("jaźń", unicode);
                }
            }
        }

        private class WithDate
        {
            public DateTime Value { get; set; }
        }

        private static readonly string[] DateTimeFormatsToRead =
        [
            "o",
            "yyyy'-'MM'-'dd'T'HH':'mm':'ss.fffffff",
            "yyyy-MM-ddTHH:mm:ss.fffffffzzz",
            "yyyy-MM-ddTHH:mm:ss.FFFFFFFK",
            "r",
            "yyyy-MM-ddTHH:mm:ss.fffK",
            "yyyy-MM-ddTHH:mm:ss.FFFK"
        ];

        public static IEnumerable<object[]> DateTimeTexts()
        {
            string[] fractions = ["", ".", ".1", ".12", ".123", ".1234", ".12345", ".123456", ".1234567", ".12345678", ".0000000", ".9999999"];
            string[] suffixes = ["", "Z", "z", "+00:00", "+02:00", "-05:30"];

            foreach (string fraction in fractions)
            foreach (string suffix in suffixes)
                yield return ["2026-04-11T12:34:56" + fraction + suffix];

            yield return ["2024-02-29T23:59:59.9999999Z"];
            yield return ["0001-01-01T00:00:00.0000000"];
            yield return ["0001-01-01T00:00:00Z"];
            yield return ["9999-12-31T23:59:59.9999999Z"];
            yield return ["9999-12-31T23:59:59.9999999"];
            yield return ["2025-02-29T00:00:00Z"];
            yield return ["2026-02-30T00:00:00.0000000Z"];
            yield return ["2026-13-01T00:00:00Z"];
            yield return ["2026-00-10T00:00:00Z"];
            yield return ["2026-04-00T00:00:00Z"];
            yield return ["0000-04-11T00:00:00Z"];
            yield return ["2026-04-11T24:00:00Z"];
            yield return ["2026-04-11T12:60:00Z"];
            yield return ["2026-04-11T12:34:60Z"];
            yield return ["2026-04-11T12:34:5Z"];
            yield return ["2026-04-11T12:34:5"];
            yield return ["2026-04-11 12:34:56Z"];
            yield return ["2026/04/11T12:34:56Z"];
            yield return ["2026-04-11T12.34.56Z"];
            yield return ["2026-04-11T12:34:56,123Z"];
            yield return ["2026-04-11T12:34:56.12a4567Z"];
            yield return ["2026-04-11T1a:34:56.1234567Z"];
            yield return ["2026-04-11T12:34:56.1234567ZZ"];
            yield return [" 2026-04-11T12:34:56.1234567Z"];
            yield return ["2026-04-11T12:34:56.1234567Z "];
            yield return ["2026-04-11"];
            yield return ["Sat, 11 Apr 2026 12:34:56 GMT"];
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [MemberData(nameof(DateTimeTexts))]
        public void SystemTextJsonReadsDateTimeLikeTheSupportedFormats(string text)
        {
            var conventions = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };

            bool parsed = DateTime.TryParseExact(text, DateTimeFormatsToRead, System.Globalization.CultureInfo.InvariantCulture, System.Globalization.DateTimeStyles.RoundtripKind, out DateTime expected);
            if (parsed && text.EndsWith("+00:00", StringComparison.Ordinal))
                expected = expected.ToUniversalTime();

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (BlittableJsonReaderObject json = context.ReadObject(new DynamicJsonValue { [nameof(WithDate.Value)] = text }, "date"))
            {
                if (parsed == false)
                {
                    Assert.ThrowsAny<Exception>(() => conventions.Serialization.DefaultConverter.FromBlittable<WithDate>(json, "dates/1"));
                    return;
                }

                DateTime actual = conventions.Serialization.DefaultConverter.FromBlittable<WithDate>(json, "dates/1").Value;

                Assert.Equal(expected.Ticks, actual.Ticks);
                Assert.Equal(expected.Kind, actual.Kind);
            }
        }

        private class WithIdAndTypedCollection
        {
            public string Id { get; set; }

            public IEnumerable<object> Items { get; set; }
        }

        private class WithTypedCollection
        {
            public IEnumerable<object> Items { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void SystemTextJsonDefaultConverterSimplifiesTypedCollectionsOfEntitiesWithIdentity()
        {
            var conventions = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                using (BlittableJsonReaderObject json = conventions.Serialization.DefaultConverter.ToBlittable(new WithTypedCollection { Items = new object[] { 1, "two" } }, context))
                {
                    var items = Assert.IsType<BlittableJsonReaderObject>(json[nameof(WithTypedCollection.Items)]);
                    Assert.True(items.TryGet(Raven.Client.Constants.Json.Fields.Type, out string _));
                }

                using (BlittableJsonReaderObject json = conventions.Serialization.DefaultConverter.ToBlittable(new WithIdAndTypedCollection { Id = "items/1", Items = new object[] { 1, "two" } }, context))
                {
                    var items = Assert.IsType<BlittableJsonReaderArray>(json[nameof(WithIdAndTypedCollection.Items)]);
                    Assert.Equal(2, items.Length);
                    Assert.Equal("items/1", json[nameof(WithIdAndTypedCollection.Id)].ToString());
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData(BlittableJsonDocumentBuilder.UsageMode.None)]
        [InlineData(BlittableJsonDocumentBuilder.UsageMode.CompressStrings)]
        public void SystemTextJsonReadsTheSameJsonTextTheBlittableWrites(BlittableJsonDocumentBuilder.UsageMode usageMode)
        {
            var conventions = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };

            var manyProperties = new DynamicJsonValue();
            for (int i = 0; i < 100; i++)
                manyProperties["Property" + i] = i;

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (BlittableJsonReaderObject embedded = context.ReadObject(new DynamicJsonValue { ["Inner"] = "value", ["Number"] = 1.5 }, "embedded"))
            {
                var document = new DynamicJsonValue
                {
                    ["Empty"] = string.Empty,
                    ["Plain"] = "plain",
                    ["Escaped"] = "quote \" backslash \\ newline \n tab \t slash / done",
                    ["ControlCharacter"] = "\u0001",
                    ["ControlCharacters"] = "a\u0001b\u001fc",
                    ["Unicode"] = "zażółć gęślą jaźń 你好 😀",
                    ["Long"] = new string('a', 300) + "\"" + new string('b', 300),
                    ["VeryLong"] = new string('c', 70_000) + "\n" + new string('d', 70_000),
                    ["Quoted \" name"] = 1,
                    ["Zero"] = 0,
                    ["Negative"] = -42,
                    ["MinLong"] = long.MinValue,
                    ["MaxLong"] = long.MaxValue,
                    ["Double"] = 1.5,
                    ["Decimal"] = 12345.6789m,
                    ["True"] = true,
                    ["False"] = false,
                    ["Null"] = null,
                    ["EmptyObject"] = new DynamicJsonValue(),
                    ["EmptyArray"] = new DynamicJsonArray(),
                    ["Nested"] = new DynamicJsonValue
                    {
                        ["Name"] = "nested \" name",
                        ["Deeper"] = new DynamicJsonValue { ["Values"] = new DynamicJsonArray { 1, "two", 3.5, null, true } }
                    },
                    ["Objects"] = new DynamicJsonArray
                    {
                        new DynamicJsonValue { ["Id"] = 1, ["Name"] = "first" },
                        new DynamicJsonValue { ["Id"] = 2, ["Name"] = "second", ["Extra"] = new DynamicJsonArray { new DynamicJsonArray { 1, 2 }, new DynamicJsonArray() } }
                    },
                    ["ManyProperties"] = manyProperties,
                    ["Embedded"] = embedded
                };

                using (BlittableJsonReaderObject json = context.ReadObject(document, "document", usageMode))
                {
                    var element = (System.Text.Json.JsonElement)conventions.Serialization.DeserializeEntityFromBlittable(typeof(System.Text.Json.JsonElement), json);

                    Assert.Equal(json.ToString(), element.GetRawText());
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void SystemTextJsonKeepsDocumentFieldsOfIgnoredMembersOnSave()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.Serialization = new SystemTextJsonSerializationConventions()
            });

            using (var session = store.OpenSession())
            {
                session.Store(new WithSecret { Name = "a", Secret = "s" }, "items/1");
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var item = session.Load<WithIgnoredMember>("items/1");
                Assert.Null(item.Secret);
                item.Name = "b";
                session.SaveChanges();
            }

            using (var session = store.OpenSession())
            {
                var item = session.Load<WithSecret>("items/1");
                Assert.Equal("b", item.Name);
                Assert.Equal("s", item.Secret);
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

        [RavenFact(RavenTestCategory.BulkInsert)]
        public async Task BulkInsertWritesMetadataValuesLikeTheSession()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.Serialization = new SystemTextJsonSerializationConventions()
            });

            var when = new DateTimeOffset(2026, 1, 1, 10, 0, 0, TimeSpan.FromHours(2));
            var at = new DateTime(2026, 1, 1, 10, 0, 0, DateTimeKind.Utc);

            await using (var bulkInsert = store.BulkInsert())
            {
                await bulkInsert.StoreAsync(new User { Name = "bulk" }, "users/bulk", new Raven.Client.Json.MetadataAsDictionary
                {
                    ["When"] = when,
                    ["Info"] = new Dictionary<string, object> { ["At"] = at }
                });
            }

            using (var session = store.OpenAsyncSession())
            {
                var user = new User { Name = "session" };
                await session.StoreAsync(user, "users/session");

                var metadata = session.Advanced.GetMetadataFor(user);
                metadata["When"] = when;
                metadata["Info"] = new Dictionary<string, object> { ["At"] = at };
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var bulk = (BlittableJsonReaderObject)(await session.LoadAsync<BlittableJsonReaderObject>("users/bulk"))[Raven.Client.Constants.Documents.Metadata.Key];
                var stored = (BlittableJsonReaderObject)(await session.LoadAsync<BlittableJsonReaderObject>("users/session"))[Raven.Client.Constants.Documents.Metadata.Key];

                Assert.Equal(stored["When"].ToString(), bulk["When"].ToString());
                Assert.Equal(((BlittableJsonReaderObject)stored["Info"])["At"].ToString(), ((BlittableJsonReaderObject)bulk["Info"])["At"].ToString());
            }
        }

        [RavenFact(RavenTestCategory.BulkInsert)]
        public async Task NamedFloatingPointMetadataValuesAreWrittenLikeNewtonsoft()
        {
            var metadata = new Raven.Client.Json.MetadataAsDictionary
            {
                ["NaN"] = double.NaN,
                ["PositiveInfinity"] = double.PositiveInfinity,
                ["NegativeInfinity"] = float.NegativeInfinity,
                ["Finite"] = 1.5
            };

            var newtonsoft = new DocumentConventions { Serialization = new NewtonsoftJsonSerializationConventions() };
            var systemTextJson = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (BlittableJsonReaderObject expected = newtonsoft.Serialization.DefaultConverter.ToBlittable(new User { Name = "user" }, metadata, context))
            using (BlittableJsonReaderObject actual = systemTextJson.Serialization.DefaultConverter.ToBlittable(new User { Name = "user" }, metadata, context))
            {
                var expectedMetadata = (BlittableJsonReaderObject)expected[Raven.Client.Constants.Documents.Metadata.Key];
                var actualMetadata = (BlittableJsonReaderObject)actual[Raven.Client.Constants.Documents.Metadata.Key];

                foreach (string key in metadata.Keys)
                {
                    Assert.IsType<LazyNumberValue>(actualMetadata[key]);
                    Assert.Equal(expectedMetadata[key].ToString(), actualMetadata[key].ToString());
                }
            }

            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.Serialization = new SystemTextJsonSerializationConventions()
            });

            await using (var bulkInsert = store.BulkInsert())
                await bulkInsert.StoreAsync(new User { Name = "bulk" }, "users/bulk", metadata);

            using (var session = store.OpenAsyncSession())
            {
                var stored = (BlittableJsonReaderObject)(await session.LoadAsync<BlittableJsonReaderObject>("users/bulk"))[Raven.Client.Constants.Documents.Metadata.Key];

                Assert.Equal("NaN", stored["NaN"].ToString());
                Assert.Equal("Infinity", stored["PositiveInfinity"].ToString());
                Assert.Equal("-Infinity", stored["NegativeInfinity"].ToString());
                Assert.Equal("1.5", stored["Finite"].ToString());
            }
        }

        [RavenFact(RavenTestCategory.BulkInsert)]
        public async Task DynamicJsonValueMetadataIsWrittenLikeNewtonsoft()
        {
            var metadata = new Raven.Client.Json.MetadataAsDictionary
            {
                ["Info"] = new DynamicJsonValue { ["a"] = 1, ["Tags"] = new DynamicJsonArray { "x", "y" } },
                ["Names"] = new Dictionary<string, string> { ["first"] = "John" }
            };

            var newtonsoft = new DocumentConventions { Serialization = new NewtonsoftJsonSerializationConventions() };
            var systemTextJson = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (BlittableJsonReaderObject expected = newtonsoft.Serialization.DefaultConverter.ToBlittable(new User { Name = "user" }, metadata, context))
            using (BlittableJsonReaderObject actual = systemTextJson.Serialization.DefaultConverter.ToBlittable(new User { Name = "user" }, metadata, context))
            {
                Assert.Equal(expected[Raven.Client.Constants.Documents.Metadata.Key].ToString(), actual[Raven.Client.Constants.Documents.Metadata.Key].ToString());
            }

            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.Serialization = new SystemTextJsonSerializationConventions()
            });

            await using (var bulkInsert = store.BulkInsert())
                await bulkInsert.StoreAsync(new User { Name = "bulk" }, "users/bulk", metadata);

            using (var session = store.OpenAsyncSession())
            {
                var stored = (BlittableJsonReaderObject)(await session.LoadAsync<BlittableJsonReaderObject>("users/bulk"))[Raven.Client.Constants.Documents.Metadata.Key];
                var info = (BlittableJsonReaderObject)stored["Info"];

                Assert.Equal(1L, info["a"]);
                Assert.Equal(2, ((BlittableJsonReaderArray)info["Tags"]).Length);
                Assert.Equal("John", ((BlittableJsonReaderObject)stored["Names"])["first"].ToString());
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

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData(false)]
        [InlineData(true)]
        public async Task SessionStoresNamedFloatingPointLiterals(bool useSystemTextJson)
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = useSystemTextJson
                        ? new SystemTextJsonSerializationConventions()
                        : new NewtonsoftJsonSerializationConventions();
                }
            });

            var literals = new NamedLiterals
            {
                NaN = double.NaN,
                PositiveInfinity = double.PositiveInfinity,
                NegativeInfinity = float.NegativeInfinity,
                Text = "NaN \"Infinity\" -Infinity"
            };

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(literals, "literals/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var read = await session.LoadAsync<NamedLiterals>("literals/1");

                Assert.True(double.IsNaN(read.NaN));
                Assert.Equal(double.PositiveInfinity, read.PositiveInfinity);
                Assert.Equal(float.NegativeInfinity, read.NegativeInfinity);
                Assert.Equal(literals.Text, read.Text);
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData(false)]
        [InlineData(true)]
        public async Task SessionStoresExponentNumbersWithoutLosingPrecision(bool useSystemTextJson)
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = useSystemTextJson
                        ? new SystemTextJsonSerializationConventions()
                        : new NewtonsoftJsonSerializationConventions();
                }
            });

            var numbers = new ExponentNumbers
            {
                Epsilon = double.Epsilon,
                Small = 1.2345678901234567E-15,
                Tiny = 1e-30,
                Huge = 1e300,
                Fraction = 0.30000000000000004,
                Single = 1.5E-20f,
                Decimal = 0.1234567890123456789012345678m
            };

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(numbers, "numbers/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var read = await session.LoadAsync<ExponentNumbers>("numbers/1");

                Assert.Equal(numbers.Epsilon, read.Epsilon);
                Assert.Equal(numbers.Small, read.Small);
                Assert.Equal(numbers.Tiny, read.Tiny);
                Assert.Equal(numbers.Huge, read.Huge);
                Assert.Equal(numbers.Fraction, read.Fraction);
                Assert.Equal(numbers.Single, read.Single);
                Assert.Equal(numbers.Decimal, read.Decimal);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        public void QueryParametersOfRavenTypesUseTheirOwnConventions()
        {
            var integers = new DocumentConventions { SaveEnumsAsIntegers = true, Serialization = new SystemTextJsonSerializationConventions() };
            var strings = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };

            Assert.Equal(FacetTermSortMode.CountDesc.ToString("D"), WriteTermSortMode(integers));
            Assert.Equal(nameof(FacetTermSortMode.CountDesc), WriteTermSortMode(strings));

            static string WriteTermSortMode(DocumentConventions conventions)
            {
                var parameters = new Parameters { ["options"] = new FacetOptions { TermSortMode = FacetTermSortMode.CountDesc } };

                using (var context = JsonOperationContext.ShortTermSingleUse())
                using (BlittableJsonReaderObject json = conventions.Serialization.DefaultConverter.ToBlittable(parameters, context))
                {
                    var options = (BlittableJsonReaderObject)json["options"];
                    return options[nameof(FacetOptions.TermSortMode)].ToString();
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void EnumsSavedAsIntegersAreReadFromNamesAndNumbers()
        {
            var conventions = new DocumentConventions { SaveEnumsAsIntegers = true, Serialization = new SystemTextJsonSerializationConventions() };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (BlittableJsonReaderObject json = context.Sync.ReadForMemory("{\"Level\":\"High\",\"Maybe\":\"LOW-LEVEL\",\"History\":[0,\"high\",\"1\"],\"ByLevel\":{\"High\":1,\"low-level\":2}}", "levels"))
            {
                var levels = conventions.Serialization.DefaultConverter.FromBlittable<WithLevels>(json);

                Assert.Equal(Level.High, levels.Level);
                Assert.Equal(Level.Low, levels.Maybe);
                Assert.Equal([Level.Low, Level.High, Level.High], levels.History);
                Assert.Equal(1, levels.ByLevel[Level.High]);
                Assert.Equal(2, levels.ByLevel[Level.Low]);

                using (BlittableJsonReaderObject written = conventions.Serialization.DefaultConverter.ToBlittable(levels, context))
                {
                    Assert.Equal(1L, written[nameof(WithLevels.Level)]);
                    Assert.Equal(0L, written[nameof(WithLevels.Maybe)]);
                    Assert.Equal([0L, 1L, 1L], ((BlittableJsonReaderArray)written[nameof(WithLevels.History)]).Cast<long>());
                    Assert.Equal(["High", "low-level"], ((BlittableJsonReaderObject)written[nameof(WithLevels.ByLevel)]).GetPropertyNames().OrderBy(x => x, StringComparer.Ordinal));
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void DerivedValuesThatConvertersWriteAsScalarsAreWrittenWithoutTypeNames()
        {
            var conventions = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (BlittableJsonReaderObject json = conventions.Serialization.DefaultConverter.ToBlittable(new Drawing { Main = new Circle { Radius = 5 } }, context))
                Assert.Equal("r=5", json[nameof(Drawing.Main)].ToString());
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void DynamicValuesWrappingNullAreEqual()
        {
            var value = new Raven.Client.Json.JsonDynamicValue(null);
            var other = new Raven.Client.Json.JsonDynamicValue(null);

            Assert.True(value.Equals(value));
            Assert.True(value.Equals(other));
            Assert.Equal(value.GetHashCode(), other.GetHashCode());
            Assert.False(value.Equals(new Raven.Client.Json.JsonDynamicValue(1L)));
            Assert.False(value.Equals((Raven.Client.Json.JsonDynamicValue)null));
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void PropertyNamingPolicyRequiresPropertyNameConverter()
        {
            var withoutConverter = new DocumentConventions
            {
                Serialization = new SystemTextJsonSerializationConventions { CustomizeJsonSerializerOptions = o => o.PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase }
            };

            var exception = Assert.Throws<NotSupportedException>(() => GetStoredNames(withoutConverter));
            Assert.Contains(nameof(DocumentConventions.PropertyNameConverter), exception.Message);

            var withConverter = new DocumentConventions
            {
                PropertyNameConverter = member => member.Name.ToLowerInvariant(),
                Serialization = new SystemTextJsonSerializationConventions { CustomizeJsonSerializerOptions = o => o.PropertyNamingPolicy = System.Text.Json.JsonNamingPolicy.CamelCase }
            };

            Assert.Equal(["name"], GetStoredNames(withConverter));

            static string[] GetStoredNames(DocumentConventions conventions)
            {
                using (var context = JsonOperationContext.ShortTermSingleUse())
                using (BlittableJsonReaderObject json = conventions.Serialization.DefaultConverter.ToBlittable(new Shape { Name = "a" }, context))
                    return json.GetPropertyNames();
            }
        }

        private class Shape
        {
            public string Name { get; set; }
        }

        [JsonConverter(typeof(CircleAsTextConverter))]
        private class Circle : Shape
        {
            public int Radius { get; set; }
        }

        private class CircleAsTextConverter : JsonConverter<Circle>
        {
            public override Circle Read(ref System.Text.Json.Utf8JsonReader reader, Type typeToConvert, System.Text.Json.JsonSerializerOptions options) =>
                new Circle { Radius = int.Parse(reader.GetString().Substring(2)) };

            public override void Write(System.Text.Json.Utf8JsonWriter writer, Circle value, System.Text.Json.JsonSerializerOptions options) =>
                writer.WriteStringValue($"r={value.Radius}");
        }

        private class Drawing
        {
            public Shape Main { get; set; }
        }

        private enum Level
        {
            [System.Runtime.Serialization.EnumMember(Value = "low-level")]
            Low,

            High
        }

        private class WithLevels
        {
            public Level Level { get; set; }

            public Level? Maybe { get; set; }

            public List<Level> History { get; set; }

            public Dictionary<Level, int> ByLevel { get; set; }
        }

        private class ExponentNumbers
        {
            public double Epsilon { get; set; }

            public double Small { get; set; }

            public double Tiny { get; set; }

            public double Huge { get; set; }

            public double Fraction { get; set; }

            public float Single { get; set; }

            public decimal Decimal { get; set; }
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
                Assert.Equal(["$type", "n"], payload.GetPropertyNames());
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

        private class Animal
        {
            public string Name { get; set; }
        }

        private class Dog : Animal
        {
            public bool Barks { get; set; }
        }

        private class Owner
        {
            public string Id { get; set; }

            public Animal Pet { get; set; }
        }

        [RavenFact(RavenTestCategory.Patching)]
        public async Task SessionPatchKeepsTheRuntimeTypeOfValuesDeclaredAsABaseType()
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
                await session.StoreAsync(new Owner { Id = "owners/1", Pet = new Animal { Name = "generic" } });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.Patch<Owner, Animal>("owners/1", x => x.Pet, new Dog { Name = "Rex", Barks = true });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var owner = await session.LoadAsync<Owner>("owners/1");
                var dog = Assert.IsType<Dog>(owner.Pet);
                Assert.Equal("Rex", dog.Name);
                Assert.True(dog.Barks);
            }
        }

        private class PartialOrder
        {
            public string Name { get; set; }

            public PartialAddress ShipTo { get; set; }

            public List<PartialLine> Lines { get; set; }

            public Dictionary<string, PartialLine> LinesByKey { get; set; }
        }

        private class PartialAddress
        {
            public PartialLocation Location { get; set; }
        }

        private class PartialLocation
        {
            public double Latitude { get; set; }
        }

        private class PartialLine
        {
            public string Product { get; set; }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData(false)]
        [InlineData(true)]
        public async Task SessionPreservesNestedPropertiesMissingFromTheModel(bool useSystemTextJson)
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.Serialization = useSystemTextJson
                    ? new SystemTextJsonSerializationConventions()
                    : new NewtonsoftJsonSerializationConventions()
            });

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.Defer(new PutCommandData("orders/1", null, new DynamicJsonValue
                {
                    ["Name"] = "order",
                    ["Unmapped"] = "top",
                    ["ShipTo"] = new DynamicJsonValue
                    {
                        ["City"] = "Frankfurt",
                        ["Location"] = new DynamicJsonValue { ["Latitude"] = 52.502065599999987, ["Extra"] = "location" }
                    },
                    ["Lines"] = new DynamicJsonArray
                    {
                        new DynamicJsonValue { ["Product"] = "p1", ["Discount"] = "d1" },
                        new DynamicJsonValue { ["Product"] = "p2", ["Discount"] = "d2" }
                    },
                    ["LinesByKey"] = new DynamicJsonValue
                    {
                        ["a"] = new DynamicJsonValue { ["Product"] = "p3", ["Discount"] = "d3" }
                    },
                    [Raven.Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Raven.Client.Constants.Documents.Metadata.Collection] = "PartialOrders"
                    }
                }));
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var order = await session.LoadAsync<PartialOrder>("orders/1");
                order.ShipTo.Location.Latitude = order.ShipTo.Location.Latitude;
                Assert.Empty(session.Advanced.WhatChanged());

                order.Name = "changed";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var raw = await session.LoadAsync<BlittableJsonReaderObject>("orders/1");
                Assert.Equal("changed", raw["Name"].ToString());
                Assert.Equal("top", raw["Unmapped"].ToString());

                var shipTo = (BlittableJsonReaderObject)raw["ShipTo"];
                Assert.Equal("Frankfurt", shipTo["City"].ToString());
                Assert.Equal("location", ((BlittableJsonReaderObject)shipTo["Location"])["Extra"].ToString());

                var lines = (BlittableJsonReaderArray)raw["Lines"];
                Assert.Equal("d1", ((BlittableJsonReaderObject)lines[0])["Discount"].ToString());
                Assert.Equal("d2", ((BlittableJsonReaderObject)lines[1])["Discount"].ToString());

                var linesByKey = (BlittableJsonReaderObject)raw["LinesByKey"];
                Assert.Equal("d3", ((BlittableJsonReaderObject)linesByKey["a"])["Discount"].ToString());
            }
        }

        private class WithExtensionData
        {
            public string Id { get; set; }

            public string Name { get; set; }

            [JsonExtensionData]
            public Dictionary<string, object> Extra { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task SessionStoresAndLoadsSystemTextJsonExtensionData()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.Serialization = new SystemTextJsonSerializationConventions()
            });

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new WithExtensionData
                {
                    Name = "name",
                    Extra = new Dictionary<string, object> { ["Age"] = 30, ["City"] = "Hadera" }
                }, "items/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var raw = await session.LoadAsync<BlittableJsonReaderObject>("items/1");
                Assert.Equal("name", raw["Name"].ToString());
                Assert.Equal(30L, raw["Age"]);
                Assert.Equal("Hadera", raw["City"].ToString());
                Assert.False(raw.TryGet(nameof(WithExtensionData.Extra), out object _));
            }

            using (var session = store.OpenAsyncSession())
            {
                var item = await session.LoadAsync<WithExtensionData>("items/1");
                Assert.Equal(30L, item.Extra["Age"]);
                Assert.Equal("Hadera", item.Extra["City"]);
                Assert.False(session.Advanced.HasChanges);
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task SessionSavesEditsToSystemTextJsonExtensionData()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.Serialization = new SystemTextJsonSerializationConventions()
            });

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.Defer(new PutCommandData("items/1", null, new DynamicJsonValue
                {
                    ["Name"] = "name",
                    ["Age"] = 30,
                    ["City"] = "Hadera",
                    [Raven.Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Raven.Client.Constants.Documents.Metadata.Collection] = "Items"
                    }
                }));
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var item = await session.LoadAsync<WithExtensionData>("items/1");
                Assert.False(session.Advanced.HasChanges);

                item.Extra["City"] = "Tel Aviv";
                item.Extra.Remove("Age");
                item.Extra["Phone"] = "050-1234";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var raw = await session.LoadAsync<BlittableJsonReaderObject>("items/1");
                Assert.Equal("Tel Aviv", raw["City"].ToString());
                Assert.Equal("050-1234", raw["Phone"].ToString());
                Assert.False(raw.TryGet("Age", out object _));
            }
        }

        private class NoIdentityWithExtensionData
        {
            public string Name { get; set; }

            [JsonExtensionData]
            public Dictionary<string, object> Extra { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task SessionSavesMetadataChangesOfEntitiesWithSystemTextJsonExtensionDataAndNoIdentity()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.Serialization = new SystemTextJsonSerializationConventions()
            });

            using (var session = store.OpenAsyncSession())
            {
                var item = new NoIdentityWithExtensionData { Name = "name", Extra = new Dictionary<string, object> { ["City"] = "Hadera" } };
                await session.StoreAsync(item, "items/1");
                session.Advanced.GetMetadataFor(item)["Custom"] = "old";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var item = await session.LoadAsync<NoIdentityWithExtensionData>("items/1");
                Assert.False(session.Advanced.HasChanges);

                session.Advanced.GetMetadataFor(item)["Custom"] = "new";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var item = await session.LoadAsync<NoIdentityWithExtensionData>("items/1");
                Assert.Equal("new", session.Advanced.GetMetadataFor(item)["Custom"]);
                Assert.Equal("Hadera", item.Extra["City"]);
            }

            using (var session = store.OpenAsyncSession())
            {
                var raw = await session.LoadAsync<BlittableJsonReaderObject>("items/1");
                Assert.Equal(1, raw.GetPropertyNames().Count(x => x == Raven.Client.Constants.Documents.Metadata.Key));
                Assert.Equal("new", ((BlittableJsonReaderObject)raw[Raven.Client.Constants.Documents.Metadata.Key])["Custom"].ToString());
            }
        }

        private class WithNewtonsoftExtensionData
        {
            public string Id { get; set; }

            public string Name { get; set; }

            [Newtonsoft.Json.JsonExtensionData]
            public IDictionary<string, object> Extra { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task NewtonsoftExtensionDataLoadsMetadataAndKeepsItAndTheRootIdentityOutOfTheDocument()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.Serialization = new SystemTextJsonSerializationConventions()
            });

            using (var session = store.OpenAsyncSession())
            {
                session.Advanced.Defer(new PutCommandData("users/1", null, new DynamicJsonValue
                {
                    ["Name"] = "name",
                    ["Age"] = 30,
                    ["City"] = "Hadera",
                    [Raven.Client.Constants.Documents.Metadata.Key] = new DynamicJsonValue
                    {
                        [Raven.Client.Constants.Documents.Metadata.Collection] = "Users"
                    }
                }));
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var user = await session.LoadAsync<WithNewtonsoftExtensionData>("users/1");
                Assert.Equal(["@metadata", "Age", "City"], user.Extra.Keys.OrderBy(x => x, StringComparer.Ordinal));
                Assert.False(session.Advanced.HasChanges);

                var metadata = Assert.IsAssignableFrom<IDictionary<string, object>>(user.Extra[Raven.Client.Constants.Documents.Metadata.Key]);
                Assert.Equal("users/1", ((Raven.Client.Json.JsonDynamicValue)metadata[Raven.Client.Constants.Documents.Metadata.Id]).Value);

                metadata[Raven.Client.Constants.Documents.Metadata.Collection] = "Hacked";
                user.Extra["Id"] = "users/2";
                user.Extra[Raven.Client.Constants.Documents.Metadata.Id] = "users/2";
                user.Extra["Phone"] = "050-1234";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var raw = await session.LoadAsync<BlittableJsonReaderObject>("users/1");
                Assert.Equal("050-1234", raw["Phone"].ToString());
                Assert.Equal(30L, raw["Age"]);
                Assert.Equal("Hadera", raw["City"].ToString());
                Assert.False(raw.TryGet("Id", out object _));
                Assert.False(raw.TryGet(Raven.Client.Constants.Documents.Metadata.Id, out object _));
                Assert.Equal("Users", ((BlittableJsonReaderObject)raw[Raven.Client.Constants.Documents.Metadata.Key])[Raven.Client.Constants.Documents.Metadata.Collection].ToString());
                Assert.Null(await session.LoadAsync<WithNewtonsoftExtensionData>("users/2"));
            }
        }

        private class ExtensibleBase
        {
        }

        private class ExtensibleDerived : ExtensibleBase
        {
            public string Name { get; set; }

            [JsonExtensionData]
            public Dictionary<string, object> Extra { get; set; }
        }

        private class NewtonsoftExtensibleDerived : ExtensibleBase
        {
            public string Name { get; set; }

            [Newtonsoft.Json.JsonExtensionData]
            public IDictionary<string, object> Extra { get; set; }
        }

        private class HoldsExtensible
        {
            public ExtensibleBase Item { get; set; }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData(false)]
        [InlineData(true)]
        public void PolymorphicValuesWithExtensionDataKeepTheirTypeAcrossRoundTrips(bool newtonsoftExtensionData)
        {
            var conventions = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };
            var holder = new HoldsExtensible
            {
                Item = newtonsoftExtensionData ? new NewtonsoftExtensibleDerived { Name = "name" } : new ExtensibleDerived { Name = "name" }
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                for (int i = 0; i < 2; i++)
                {
                    using (BlittableJsonReaderObject json = conventions.Serialization.DefaultConverter.ToBlittable(holder, context))
                        holder = conventions.Serialization.DefaultConverter.FromBlittable<HoldsExtensible>(json);
                }
            }

            IDictionary<string, object> extra = holder.Item switch
            {
                ExtensibleDerived derived => derived.Extra,
                NewtonsoftExtensibleDerived derived => derived.Extra,
                _ => throw new InvalidOperationException($"Expected a derived item but got {holder.Item?.GetType()}")
            };

            Assert.Equal("name", newtonsoftExtensionData ? ((NewtonsoftExtensibleDerived)holder.Item).Name : ((ExtensibleDerived)holder.Item).Name);
            Assert.False(extra?.ContainsKey("$type") ?? false);
        }

        [RavenFact(RavenTestCategory.Vector | RavenTestCategory.Querying)]
        public void VectorSearchFieldNamesFollowStoreConventions()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions();
                    s.Conventions.PropertyNameConverter = member => char.ToLowerInvariant(member.Name[0]) + member.Name.Substring(1);
                }
            });

            using (var session = store.OpenSession())
            {
                string linq = session.Query<User>()
                    .VectorSearch(f => f.WithText(x => x.Name), v => v.ByText("x"))
                    .ToString();

                string documentQuery = session.Advanced.DocumentQuery<User>()
                    .VectorSearch(f => f.WithText(x => x.Name), v => v.ByText("x"))
                    .ToString();

                Assert.Contains("embedding.text(name)", linq);
                Assert.Contains("embedding.text(name)", documentQuery);
            }
        }

        [RavenFact(RavenTestCategory.Querying | RavenTestCategory.ClientApi)]
        public async Task NewtonsoftRenamesAreUsedForStorageAndQueries()
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
                await session.StoreAsync(new NewtonsoftRenamed { Id = "items/1", Title = "Oren" });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var raw = await session.LoadAsync<BlittableJsonReaderObject>("items/1");
                Assert.True(raw.TryGet("title", out string title));
                Assert.Equal("Oren", title);
            }

            using (var session = store.OpenAsyncSession())
            {
                List<NewtonsoftRenamed> results = await session.Query<NewtonsoftRenamed>()
                    .Customize(x => x.WaitForNonStaleResults())
                    .Where(x => x.Title == "Oren")
                    .ToListAsync();

                Assert.Equal("Oren", Assert.Single(results).Title);
            }
        }

        private class NewtonsoftRenamed
        {
            public string Id { get; set; }

            [Newtonsoft.Json.JsonProperty("title")]
            public string Title { get; set; }
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

        private sealed class FullChild
        {
            public string Name { get; set; }

            public string Extra { get; set; }
        }

        private sealed class FullParent
        {
            public string Name { get; set; }

            public string Extra { get; set; }

            public FullChild Child { get; set; }

            public List<FullChild> Children { get; set; }

            public Dictionary<string, FullChild> ChildrenByKey { get; set; }

            public List<List<FullChild>> NestedChildren { get; set; }
        }

        private sealed class PartialChild
        {
            public string Name { get; set; }
        }

        private sealed class PartialParent
        {
            public string Name { get; set; }

            public PartialChild Child { get; set; }

            public List<PartialChild> Children { get; set; }

            public Dictionary<string, PartialChild> ChildrenByKey { get; set; }

            public List<List<PartialChild>> NestedChildren { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task PreserveDocumentPropertiesNotFoundOnModel_RoundTripsNestedObjects()
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
                await session.StoreAsync(new FullParent
                {
                    Name = "parent",
                    Extra = "parent extra",
                    Child = new FullChild { Name = "child", Extra = "child extra" },
                    Children =
                    [
                        new FullChild { Name = "first", Extra = "first extra" },
                        new FullChild { Name = "second", Extra = "second extra" }
                    ],
                    ChildrenByKey = new Dictionary<string, FullChild>
                    {
                        ["key"] = new FullChild { Name = "keyed", Extra = "keyed extra" }
                    },
                    NestedChildren = [[new FullChild { Name = "nested", Extra = "nested extra" }]]
                }, "parents/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var entity = await session.LoadAsync<PartialParent>("parents/1");
                Assert.Equal("second", entity.Children[1].Name);

                entity.Name = "renamed";
                entity.Children[1].Name = "renamed second";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var raw = await session.LoadAsync<BlittableJsonReaderObject>("parents/1");

                Assert.True(raw.TryGet("Name", out string name));
                Assert.Equal("renamed", name);
                Assert.True(raw.TryGet("Extra", out string extra));
                Assert.Equal("parent extra", extra);

                Assert.True(raw.TryGet("Child", out BlittableJsonReaderObject child));
                Assert.True(child.TryGet("Extra", out extra));
                Assert.Equal("child extra", extra);

                Assert.True(raw.TryGet("Children", out BlittableJsonReaderArray children));
                var second = (BlittableJsonReaderObject)children[1];
                Assert.True(second.TryGet("Name", out name));
                Assert.Equal("renamed second", name);
                Assert.True(second.TryGet("Extra", out extra));
                Assert.Equal("second extra", extra);
                Assert.True(((BlittableJsonReaderObject)children[0]).TryGet("Extra", out extra));
                Assert.Equal("first extra", extra);

                Assert.True(raw.TryGet("ChildrenByKey", out BlittableJsonReaderObject childrenByKey));
                Assert.True(childrenByKey.TryGet("key", out BlittableJsonReaderObject keyed));
                Assert.True(keyed.TryGet("Extra", out extra));
                Assert.Equal("keyed extra", extra);

                Assert.True(raw.TryGet("NestedChildren", out BlittableJsonReaderArray nestedChildren));
                var nested = (BlittableJsonReaderObject)((BlittableJsonReaderArray)nestedChildren[0])[0];
                Assert.True(nested.TryGet("Extra", out extra));
                Assert.Equal("nested extra", extra);
            }
        }

        private class FullItem
        {
            public string Name { get; set; }

            public string Extra { get; set; }
        }

        private class PartialItem : IComparable<PartialItem>
        {
            public string Name { get; set; }

            public int CompareTo(PartialItem other) => string.CompareOrdinal(Name, other?.Name);
        }

        private class FullCollections
        {
            public string Name { get; set; }

            public List<FullItem> Stacked { get; set; }

            public List<FullItem> Sorted { get; set; }

            public List<FullItem> Hashed { get; set; }
        }

        private class PartialCollections
        {
            public string Name { get; set; }

            public Stack<PartialItem> Stacked { get; set; }

            public SortedSet<PartialItem> Sorted { get; set; }

            public HashSet<PartialItem> Hashed { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task PreserveDocumentPropertiesNotFoundOnModel_KeepsPropertiesWithTheirItemInReorderingCollections()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new SystemTextJsonSerializationConventions();
                }
            });

            static List<FullItem> Items() =>
            [
                new FullItem { Name = "z", Extra = "z extra" },
                new FullItem { Name = "a", Extra = "a extra" },
                new FullItem { Name = "m", Extra = "m extra" }
            ];

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new FullCollections { Name = "holder", Stacked = Items(), Sorted = Items(), Hashed = Items() }, "collections/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var entity = await session.LoadAsync<PartialCollections>("collections/1");
                Assert.Equal(3, entity.Stacked.Count);
                Assert.Equal(3, entity.Sorted.Count);
                Assert.Equal(3, entity.Hashed.Count);

                entity.Name = "renamed";
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var raw = await session.LoadAsync<BlittableJsonReaderObject>("collections/1");
                Assert.True(raw.TryGet("Name", out string holderName));
                Assert.Equal("renamed", holderName);

                AssertExtras(raw, nameof(PartialCollections.Stacked), required: true);
                AssertExtras(raw, nameof(PartialCollections.Hashed), required: true);
                AssertExtras(raw, nameof(PartialCollections.Sorted), required: false);
            }

            static void AssertExtras(BlittableJsonReaderObject raw, string property, bool required)
            {
                Assert.True(raw.TryGet(property, out BlittableJsonReaderArray items));
                Assert.Equal(3, items.Length);

                foreach (BlittableJsonReaderObject item in items)
                {
                    Assert.True(item.TryGet("Name", out string name));
                    if (item.TryGet("Extra", out string extra) == false)
                    {
                        Assert.False(required, $"{property}: item '{name}' lost its Extra property");
                        continue;
                    }

                    Assert.Equal(name + " extra", extra);
                }
            }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void DefaultClrTypeResolutionFollowsTheContextualReflectionContext()
        {
            Type defaultType = typeof(SystemTextJsonSerializationTests);
            string typeName = defaultType.FullName + ", " + defaultType.Assembly.GetName().Name;
            var conventions = new DocumentConventions();

            Assert.Same(defaultType, conventions.ResolveTypeFromClrTypeName(typeName));

            var loadContext = new AssemblyLoadContext("clr-type-resolution", isCollectible: true);
            try
            {
                loadContext.LoadFromAssemblyPath(defaultType.Assembly.Location);
                using (loadContext.EnterContextualReflection())
                {
                    Type expected = Type.GetType(typeName);
                    Assert.NotSame(defaultType, expected);
                    Assert.Same(expected, conventions.ResolveTypeFromClrTypeName(typeName));
                }

                Assert.Same(defaultType, conventions.ResolveTypeFromClrTypeName(typeName));
            }
            finally
            {
                loadContext.Unload();
            }
        }

        private class RequiredIdentity
        {
            public required string Id { get; set; }

            public required string Name { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task CanLoadEntityWithRequiredIdentityProperty()
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
                await session.StoreAsync(new RequiredIdentity { Id = "required/1", Name = "name" });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var raw = await session.LoadAsync<BlittableJsonReaderObject>("required/1");
                Assert.False(raw.TryGetMember(nameof(RequiredIdentity.Id), out _));
            }

            using (var session = store.OpenAsyncSession())
            {
                var loaded = await session.LoadAsync<RequiredIdentity>("required/1");
                Assert.Equal("required/1", loaded.Id);
                Assert.Equal("name", loaded.Name);
            }
        }

        [JsonPolymorphic]
        [JsonDerivedType(typeof(PolymorphicDog), "dog")]
        [JsonDerivedType(typeof(PolymorphicCat), "cat")]
        private abstract class PolymorphicAnimal
        {
            public string Name { get; set; }
        }

        private class PolymorphicDog : PolymorphicAnimal
        {
            public bool Barks { get; set; }
        }

        private class PolymorphicCat : PolymorphicAnimal
        {
            public int Lives { get; set; }
        }

        private class PolymorphicOwner
        {
            public PolymorphicAnimal Pet { get; set; }

            public List<PolymorphicAnimal> Pets { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task JsonPolymorphicHierarchyIsStoredWithClrTypeNames()
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
                await session.StoreAsync(new PolymorphicOwner
                {
                    Pet = new PolymorphicDog { Name = "rex", Barks = true },
                    Pets = [new PolymorphicCat { Name = "tom", Lives = 9 }, new PolymorphicDog { Name = "fido" }]
                }, "owners/1");
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var raw = await session.LoadAsync<BlittableJsonReaderObject>("owners/1");
                Assert.True(raw.TryGet(nameof(PolymorphicOwner.Pet), out BlittableJsonReaderObject pet));
                Assert.True(pet.TryGet(Constants.Json.Fields.Type, out string typeName));
                Assert.Equal(typeof(PolymorphicDog), Type.GetType(typeName));
            }

            using (var session = store.OpenAsyncSession())
            {
                var loaded = await session.LoadAsync<PolymorphicOwner>("owners/1");
                var dog = Assert.IsType<PolymorphicDog>(loaded.Pet);
                Assert.Equal("rex", dog.Name);
                Assert.True(dog.Barks);

                Assert.Equal(2, loaded.Pets.Count);
                var cat = Assert.IsType<PolymorphicCat>(loaded.Pets[0]);
                Assert.Equal("tom", cat.Name);
                Assert.Equal(9, cat.Lives);
                Assert.IsType<PolymorphicDog>(loaded.Pets[1]);
            }
        }

        private class UntypedSlots
        {
            public object Slot { get; set; }

            public List<object> Items { get; set; }

            public Dictionary<string, object> Bag { get; set; }
        }

        [RavenTheory(RavenTestCategory.ClientApi)]
        [InlineData("System.Byte[], System.Private.CoreLib")]
        [InlineData("System.Byte[], mscorlib")]
        public void ReadsByteArraysWrittenByNewtonsoftIntoUntypedMembers(string typeName)
        {
            var conventions = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };
            string bytes = "{\"$type\":\"" + typeName + "\",\"$value\":\"AQL/\"}";
            string document = "{\"Slot\":" + bytes + ",\"Items\":[" + bytes + ",1],\"Bag\":{\"b\":" + bytes + ",\"n\":2}}";

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (BlittableJsonReaderObject json = context.Sync.ReadForMemory(document, "slots"))
            {
                var slots = conventions.Serialization.DefaultConverter.FromBlittable<UntypedSlots>(json);

                Assert.Equal(new byte[] { 1, 2, 255 }, Assert.IsType<byte[]>(slots.Slot));
                Assert.Equal(2, slots.Items.Count);
                Assert.Equal(new byte[] { 1, 2, 255 }, Assert.IsType<byte[]>(slots.Items[0]));
                Assert.Equal(1L, slots.Items[1]);
                Assert.Equal(new byte[] { 1, 2, 255 }, Assert.IsType<byte[]>(slots.Bag["b"]));
                Assert.Equal(2L, slots.Bag["n"]);
            }
        }

        private class JsonNodeMembers
        {
            public System.Text.Json.Nodes.JsonNode ArrayNode { get; set; }

            public System.Text.Json.Nodes.JsonNode ObjectNode { get; set; }

            public System.Text.Json.Nodes.JsonNode ValueNode { get; set; }

            public System.Text.Json.Nodes.JsonObject Object { get; set; }

            public System.Text.Json.Nodes.JsonArray Array { get; set; }

            public List<System.Text.Json.Nodes.JsonNode> Nodes { get; set; }

            public Dictionary<string, System.Text.Json.Nodes.JsonNode> NodesByKey { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public void JsonNodeMembersRoundTrip()
        {
            var conventions = new DocumentConventions { Serialization = new SystemTextJsonSerializationConventions() };
            var entity = new JsonNodeMembers
            {
                ArrayNode = System.Text.Json.Nodes.JsonNode.Parse("[1,{\"b\":true},\"x\",null]"),
                ObjectNode = System.Text.Json.Nodes.JsonNode.Parse("{\"a\":[1,2]}"),
                ValueNode = System.Text.Json.Nodes.JsonValue.Create(5),
                Object = new System.Text.Json.Nodes.JsonObject { ["c"] = 1, ["d"] = new System.Text.Json.Nodes.JsonArray(1, 2) },
                Array = new System.Text.Json.Nodes.JsonArray(1, "two"),
                Nodes = [System.Text.Json.Nodes.JsonNode.Parse("[1]"), System.Text.Json.Nodes.JsonNode.Parse("{\"e\":1}"), null],
                NodesByKey = new Dictionary<string, System.Text.Json.Nodes.JsonNode> { ["k"] = System.Text.Json.Nodes.JsonNode.Parse("[\"v\"]") }
            };

            using (var context = JsonOperationContext.ShortTermSingleUse())
            using (BlittableJsonReaderObject json = conventions.Serialization.DefaultConverter.ToBlittable(entity, context))
            {
                var loaded = conventions.Serialization.DefaultConverter.FromBlittable<JsonNodeMembers>(json);

                Assert.Equal("[1,{\"b\":true},\"x\",null]", loaded.ArrayNode.ToJsonString());
                Assert.Equal("{\"a\":[1,2]}", loaded.ObjectNode.ToJsonString());
                Assert.Equal("5", loaded.ValueNode.ToJsonString());
                Assert.Equal("{\"c\":1,\"d\":[1,2]}", loaded.Object.ToJsonString());
                Assert.Equal("[1,\"two\"]", loaded.Array.ToJsonString());
                Assert.Equal(3, loaded.Nodes.Count);
                Assert.Equal("[1]", loaded.Nodes[0].ToJsonString());
                Assert.Equal("{\"e\":1}", loaded.Nodes[1].ToJsonString());
                Assert.Null(loaded.Nodes[2]);
                Assert.Equal("[\"v\"]", loaded.NodesByKey["k"].ToJsonString());
            }
        }

        private class IgnoresNullIdentity
        {
            [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
            public string Id { get; set; }

            public string Name { get; set; }
        }

        private class HoldsIgnoresNullIdentity
        {
            public string Id { get; set; }

            public IgnoresNullIdentity Child { get; set; }
        }

        [RavenFact(RavenTestCategory.ClientApi)]
        public async Task IdentityPropertyKeepsItsJsonIgnoreCondition()
        {
            using var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s => s.Conventions.Serialization = new SystemTextJsonSerializationConventions()
            });

            using (var session = store.OpenAsyncSession())
            {
                await session.StoreAsync(new HoldsIgnoresNullIdentity { Id = "holders/1", Child = new IgnoresNullIdentity { Name = "child" } });
                await session.SaveChangesAsync();
            }

            using (var session = store.OpenAsyncSession())
            {
                var raw = await session.LoadAsync<BlittableJsonReaderObject>("holders/1");
                Assert.True(raw.TryGet(nameof(HoldsIgnoresNullIdentity.Child), out BlittableJsonReaderObject child));
                Assert.False(child.TryGetMember(nameof(IgnoresNullIdentity.Id), out _));
                Assert.True(child.TryGet(nameof(IgnoresNullIdentity.Name), out string name));
                Assert.Equal("child", name);
            }

            using (var context = JsonOperationContext.ShortTermSingleUse())
            {
                using (BlittableJsonReaderObject json = store.Conventions.Serialization.DefaultConverter.ToBlittable(new IgnoresNullIdentity { Name = "root" }, context))
                    Assert.False(json.TryGetMember(nameof(IgnoresNullIdentity.Id), out _));

                using (BlittableJsonReaderObject json = store.Conventions.Serialization.DefaultConverter.ToBlittable(new IgnoresNullIdentity { Id = "children/1", Name = "root" }, context))
                {
                    Assert.True(json.TryGet(nameof(IgnoresNullIdentity.Id), out string id));
                    Assert.Equal("children/1", id);
                }
            }
        }
    }
}
