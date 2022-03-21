using System;
using System.Threading.Tasks;
using FastTests;
using Newtonsoft.Json;
using Raven.Client.Documents;
using Raven.Client.Documents.Linq;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13363 : RavenTestBase
    {
        public RavenDB_13363(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanUseConvertersToSerializeQueryParameters()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = documentStore =>
                {
                    documentStore.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonSerializer = serializer => serializer.Converters.Add(new NumberJsonConverter())
                    };
                }
            }))
            {
                using (var session = store.OpenAsyncSession())
                {
                    var document1 = new TestDocument { Id = "docs/1", MyNumber = new Number("1") };
                    var document2 = new TestDocument { Id = "docs/2", MyNumber = new Number("2") };
                    await session.StoreAsync(document1);
                    await session.StoreAsync(document2);
                    await session.SaveChangesAsync();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenAsyncSession())
                {
                    var documents = await session
                        .Query<TestDocument>()
                        .Where(d => d.MyNumber == new Number("1"))
                        .ToListAsync();

                    Assert.Equal(1, documents.Count);
                }
            }
        }

        private class TestDocument
        {
            public string Id { get; set; }
            public Number MyNumber { get; set; }
        }

        private class Number
        {
            public Number(string value)
            {
                Value = value;
            }

            public string Value { get; set; }

            public override bool Equals(object obj)
            {
                return obj is Number number &&
                       Value == number.Value;
            }

            public override int GetHashCode()
            {
                return (Value ?? string.Empty).GetHashCode();
            }
        }

        private class NumberJsonConverter : JsonConverter
        {
            public override void WriteJson(JsonWriter writer, object value, JsonSerializer serializer)
            {
                var number = (Number)value;
                writer.WriteValue(number.Value);
            }

            public override object ReadJson(JsonReader reader, Type objectType, object existingValue, JsonSerializer serializer)
            {
                return reader.Value != null
                    ? new Number((string)reader.Value)
                    : null;
            }

            public override bool CanConvert(Type objectType)
            {
                return typeof(Number) == objectType;
            }
        }
    }
}
