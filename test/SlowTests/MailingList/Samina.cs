using System;
using System.Linq;
using FastTests;
using Raven.Client;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Samina : RavenTestBase
    {
        public Samina(ITestOutputHelper output) : base(output)
        {
        }


        private class Property
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int BedroomCount { get; set; }
        }

        private class Catalog
        {
            public string Id { get; set; }
            public string PropertyId { get; set; }
            public string Type { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void Can_search_with_filters(Options options)
        {
            Property property = new Property { Id = Guid.NewGuid().ToString(), Name = "Property Name", BedroomCount = 3 };
            Catalog catalog = new Catalog() { Id = Guid.NewGuid().ToString(), Type = "Waterfront", PropertyId = property.Id };

            using (var store = GetDocumentStore(options))
            using (var session = store.OpenSession())
            {

                session.Store(property);
                session.Store(catalog);
                session.SaveChanges();

                var catalogs = session.Advanced.DocumentQuery<Catalog>().WaitForNonStaleResults().WhereEquals("Type", "Waterfront").ToList().Select(c => c.PropertyId);
                var properties = session.Advanced.DocumentQuery<Property>();
                properties.OpenSubclause();
                var first = true;
                foreach (var guid in catalogs)
                {
                    if (first == false)
                        properties.OrElse();
                    properties.WhereEquals(Constants.Documents.Indexing.Fields.DocumentIdFieldName, guid);
                    first = false;
                }
                properties.CloseSubclause();
                var refinedProperties = properties.AndAlso().WhereGreaterThanOrEqual("BedroomCount", "2").ToList().Select(p => p.Id);

                Assert.NotEqual(0, refinedProperties.Count());
            }
        }
    }
}
