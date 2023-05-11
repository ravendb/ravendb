using System.Collections.Generic;
using System.Linq;
using FastTests;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Pfeffer : RavenTestBase
    {
        public Pfeffer(ITestOutputHelper output) : base(output)
        {
        }

        bool TryConvertValueForQueryDelegate(string fieldName, object value, bool forRange, out object obj)
        {
            obj = JObject.FromObject(value, new JsonSerializer
            {
                TypeNameHandling = TypeNameHandling.All
            });
            return true;
        }

        [RavenTheory(RavenTestCategory.Querying | RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene, DatabaseMode = RavenDatabaseMode.All)]
        public void QueryingUsingObjects(Options options)
        {
            options.ModifyDocumentStore = documentStore =>
                documentStore.Conventions.RegisterQueryValueConverter<object>(TryConvertValueForQueryDelegate, RangeType.None);
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())

                {
                    var obj = new Outer { Examples = new List<IExample>() { new Example { Provider = "Test", Id = "Abc" } } };
                    session.Store(obj);
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var ex = new Example { Provider = "Test", Id = "Abc" };
                    var arr = session.Query<Outer>().Customize(c => c.WaitForNonStaleResults())
                        .Where(o => o.Examples.Any(e => e == ex))
                        .ToArray();
                    //WaitForUserToContinueTheTest(store);
                    Assert.Equal(1, arr.Length);
                }
            }
        }

        // Define other methods and classes here
        private interface IExample
        {
        }

        private class Outer
        {
            public string Id { get; set; }
            public IList<IExample> Examples { get; set; }
        }

        private class Example : IExample
        {
            public string Provider { get; set; }
            public string Id { get; set; }
        }
    }
}
