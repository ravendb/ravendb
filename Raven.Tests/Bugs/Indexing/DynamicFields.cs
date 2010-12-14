using System;
using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Documents;
using Raven.Client.Indexes;
using Xunit;

namespace Raven.Tests.Bugs.Indexing
{
    public class DynamicFields : LocalClientTest
    {
        public class Product
        {
            public string Id { get; set; }
            public List<Attribute> Attributes { get; set; }
        }

        public class Attribute
        {
            public string Name { get; set; }
            public string Value { get; set; }
        }

        public class Product_ByAttribute : AbstractIndexCreationTask<Product>
        {
            public Product_ByAttribute()
            {
                Map = products =>
                    from p in products
                    select new
                    {
                        _ = Project(p.Attributes, attribute => new Field(attribute.Name, attribute.Value, Field.Store.NO, Field.Index.ANALYZED))
                    };
            }
        }

        [Fact]
        public void CanCreateCompletelyDynamicFields()
        {
            using (var store = NewDocumentStore())
            {
                new Product_ByAttribute().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Product
                    {
                        Attributes = new List<Attribute>
                        {
                            new Attribute{Name = "Color", Value = "Red"}
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var products = session.Advanced.LuceneQuery<Product>("Product/ByAttribute")
                        .WhereEquals("Color", "Red")
                        .WaitForNonStaleResults(TimeSpan.FromMinutes(3))
                        .ToList();

                    Assert.NotEmpty(products);
                }
            }
        }
    }


}