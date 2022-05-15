using FastTests;
using FastTests.Server.JavaScript;
using Newtonsoft.Json.Linq;
using Raven.Client;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12892 : RavenTestBase
    {
        public RavenDB_12892(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Name { get; set; }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void Projections_should_return_null_values_on_missing_properties(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User
                    {
                        Name = "jerry"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    const string query = "from Users select Name, BooleanValue, IntValue, DecimalValue";

                    var document = session.Advanced
                        .RawQuery<object>(query)
                        .Single();

                    AssertResult(document);

                    const string jsQuery = @"from Users as u 
                                            select { 
                                                Name : u.Name,
                                                BooleanValue: u.BooleanValue, 
                                                IntValue: u.IntValue, 
                                                DecimalValue: u.DecimalValue
                                            }";

                    document = session.Advanced
                        .RawQuery<object>(jsQuery)
                        .Single();

                    AssertResult(document);
                }
            }
        }

        private static void AssertResult(dynamic document)
        {
            var propsCount = ((JObject)document).Count;

            Assert.Equal(5, propsCount);

            Assert.Contains(Constants.Documents.Metadata.Key, document);
            Assert.Contains("Name", document);
            Assert.Contains("BooleanValue", document);
            Assert.Contains("IntValue", document);
            Assert.Contains("DecimalValue", document);

            Assert.Equal("jerry", document.Name.Value);
            Assert.Null(document.BooleanValue.Value);
            Assert.Null(document.IntValue.Value);
            Assert.Null(document.DecimalValue.Value);
        }
    }
}
