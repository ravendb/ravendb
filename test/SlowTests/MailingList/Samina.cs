using System;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.MailingList
{
    public class Samina : RavenTestBase
    {

        private class Property
        {
            public Guid Id { get; set; }
            public string Name { get; set; }
            public int BedroomCount { get; set; }
        }

        private class Catalog
        {
            public Guid Id { get; set; }
            public Guid PropertyId { get; set; }
            public string Type { get; set; }
        }

        [Fact]
        public void Can_search_with_filters()
        {
            Property property = new Property { Id = Guid.NewGuid(), Name = "Property Name", BedroomCount = 3 };
            Catalog catalog = new Catalog() { Id = Guid.NewGuid(), Type = "Waterfront", PropertyId = property.Id };

            using (var store = GetDocumentStore())
            using (var session = store.OpenSession())
            {

                session.Store(property);
                session.Store(catalog);
                session.SaveChanges();

                var catalogs = session.Advanced.DocumentQuery<Catalog>().WhereEquals("Type", "Waterfront").Select(c => c.PropertyId);
                var properties = session.Advanced.DocumentQuery<Property>();
                properties.OpenSubclause();
                var first = true;
                foreach (var guid in catalogs)
                {
                    if (first == false)
                        properties.OrElse();
                    properties.WhereEquals("__document_id", guid);
                    first = false;
                }
                properties.CloseSubclause();
                var refinedProperties = properties.AndAlso().WhereGreaterThanOrEqual("BedroomCount", "2").Select(p => p.Id);

                Assert.NotEqual(0, refinedProperties.Count());
            }
        }
    }
}
