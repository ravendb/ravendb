using System.Collections.Generic;
using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Tests.Linq
{
    public class SelectDictionaryItem : RavenTestBase
    {
        public SelectDictionaryItem(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All, SearchEngineMode = RavenSearchEngineMode.All)]
        public void SupportProjectionOnDictionaryField(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Product
                    {
                        Properties = new Dictionary<string, string>
                        {
                            {"Vendor", "Hibernating Rhinos"},
                        }
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var vendor = session.Query<Product>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .Select(product => product.Properties["Vendor"])
                        .FirstOrDefault();

                    Assert.Equal("Hibernating Rhinos", vendor);
                }
            }
        }

        private class Product
        {
            public string Id { get; set; }
            public Dictionary<string, string> Properties { get; set; }
        }
    }
}