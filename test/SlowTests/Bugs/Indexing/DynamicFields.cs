using System;
using System.Collections.Generic;
using System.Linq;
using FastTests;
using Lucene.Net.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Bugs.Indexing
{
    public class DynamicFields : RavenTestBase
    {
        private class Product
        {
            public string Id { get; set; }
            public List<Attribute> Attributes { get; set; }
        }

        private class Attribute
        {
            public string Name { get; set; }
            public string Value { get; set; }
            public decimal NumericValue { get; set; }
            public int IntValue { get; set; }
        }

        private class Product_ByAttribute : AbstractIndexCreationTask<Product>
        {
            public Product_ByAttribute()
            {
                Map = products =>
                    from p in products
                    select new
                    {
                        _ = p.Attributes.Select(attribute => new Field(attribute.Name, attribute.Value, Field.Store.NO, Field.Index.ANALYZED))
                    };
            }
        }

        private class Product_ByNumericAttribute : AbstractIndexCreationTask<Product>
        {
            public Product_ByNumericAttribute()
            {
                Map = products =>
                    from p in products
                    select new
                    {
                        _ = p.Attributes.Select(attribute => new NumericField(attribute.Name + "_D_Range", Field.Store.NO, true).SetDoubleValue((double)attribute.NumericValue))
                    };
            }
        }

        private class Product_ByNumericAttributeUsingField : AbstractIndexCreationTask<Product>
        {
            public Product_ByNumericAttributeUsingField()
            {
                Map = products =>
                    from p in products
                    select new
                    {
                        _ = p.Attributes.Select(attribute => new Field(attribute.Name, attribute.NumericValue.ToString("#.#"), Field.Store.YES, Field.Index.NOT_ANALYZED_NO_NORMS))
                    };
            }
        }

        private class Product_ByIntAttribute : AbstractIndexCreationTask<Product>
        {
            public Product_ByIntAttribute()
            {
                Map = products =>
                    from p in products
                    select new
                    {
                        _ = p.Attributes.Select(attribute => new NumericField(attribute.Name + "_L_Range", Field.Store.NO, true).SetLongValue(attribute.IntValue))
                    };
            }
        }


        [Fact]
        public void CanCreateCompletelyDynamicFields()
        {
            using (var store = GetDocumentStore())
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
                    var products = session.Advanced.DocumentQuery<Product>("Product/ByAttribute")
                        .WhereEquals("Color", "Red")
                        .WaitForNonStaleResults(TimeSpan.FromMinutes(3))
                        .ToList();

                    Assert.NotEmpty(products);
                }
            }
        }

        [Fact]
        public void CanCreateCompletelyDynamicNumericFields()
        {
            using (var store = GetDocumentStore())
            {
                new Product_ByNumericAttribute().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Product
                    {
                        Attributes = new List<Attribute>
                        {
                            new Attribute{Name = "Color", Value = "Red", NumericValue = 30}
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var products = session.Advanced.DocumentQuery<Product, Product_ByNumericAttribute>()
                        .WhereGreaterThan("Color", 20d)
                        .WaitForNonStaleResults(TimeSpan.FromMinutes(3))
                        .ToList();

                    Assert.NotEmpty(products);
                }
            }
        }

        [Fact]
        public void CanCreateCompletelyDynamicNumericFieldsUsingField()
        {
            using (var store = GetDocumentStore())
            {
                new Product_ByNumericAttributeUsingField().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Product
                    {
                        Attributes = new List<Attribute>
                        {
                            new Attribute{Name = "Color", Value = "Red", NumericValue = 30}
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var products = session.Advanced.DocumentQuery<Product, Product_ByNumericAttributeUsingField>()
                        .WhereEquals("Color", "30")
                        .WaitForNonStaleResults(TimeSpan.FromMinutes(3))
                        .ToList();

                    Assert.NotEmpty(products);
                }
            }
        }

        [Fact]
        public void CanQueryCompletelyDynamicNumericFieldsWithNegativeRangeUsingInt()
        {
            using (var store = GetDocumentStore())
            {
                new Product_ByIntAttribute().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Product
                    {
                        Attributes = new List<Attribute>
                        {
                            new Attribute{Name = "Color", Value = "Red", IntValue = 30}
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var products = session.Advanced.DocumentQuery<Product, Product_ByIntAttribute>()
                        .WhereGreaterThan("Color", -1)
                        .WaitForNonStaleResults(TimeSpan.FromMinutes(3))
                        .ToList();

                    Assert.NotEmpty(products);
                }
            }
        }

        [Fact]
        public void CanQueryCompletelyDynamicNumericFieldsWithNegativeRange()
        {
            using (var store = GetDocumentStore())
            {
                new Product_ByNumericAttribute().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Product
                    {
                        Attributes = new List<Attribute>
                        {
                            new Attribute{Name = "Color", Value = "Red", NumericValue = 30}
                        }
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var products = session.Advanced.DocumentQuery<Product, Product_ByNumericAttribute>()
                        .WhereGreaterThan("Color", -1d)
                        .WaitForNonStaleResults(TimeSpan.FromMinutes(3))
                        .ToList();

                    Assert.NotEmpty(products);
                }
            }
        }
    }


}
