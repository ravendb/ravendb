using System.Linq;
using FastTests;
using FastTests.Server.JavaScript;
using Newtonsoft.Json;
using Raven.Client.Json.Serialization.NewtonsoftJson;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12484 : RavenTestBase
    {
        public RavenDB_12484(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void ProjectionAgainstMissingBasicValueTypes(Options options)
        {
            const string documentId = "document-id";

            using (var store = GetDocumentStore(new Options
            {
                ModifyDocumentStore = s =>
                {
                    s.Conventions.Serialization = new NewtonsoftJsonSerializationConventions
                    {
                        CustomizeJsonDeserializer = des => des.NullValueHandling = NullValueHandling.Ignore
                    };
                },
                ModifyDatabaseRecord = Options.ModifyForJavaScriptEngine(jsEngineType)
            }))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Document
                    {
                        Id = documentId
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var queryable = s.Query<Document>()
                        .Where(x => x.Id == documentId)
                        .Select(x => new Result
                        {
                            // this generates a JS projection
                            FailedMin = 0,
                            BooleanValue = x.BooleanValue,
                            IntValue = x.IntValue,
                            DecimalValue = x.DecimalValue
                        });

                    var stats = queryable
                        .SingleOrDefault();

                    Assert.NotNull(stats);
                    Assert.False(stats.BooleanValue);
                    Assert.Equal(0, stats.IntValue);
                    Assert.Equal(0, stats.DecimalValue);
                }
            }
        }

        private class Result
        {
            public int? FailedMin { get; set; }
            public bool BooleanValue { get; set; }
            public int IntValue { get; set; }
            public decimal DecimalValue { get; set; }
        }

        private class Document
        {
            public string Id { get; set; }

            [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
            public bool BooleanValue { get; set; }

            [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
            public int IntValue { get; set; }

            [JsonProperty(DefaultValueHandling = DefaultValueHandling.Ignore)]
            public decimal DecimalValue { get; set; }
        }
    }
}
