using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Raven.Client.Documents.Transformers;
using Xunit;

namespace SlowTests.Bugs.LiveProjections
{
    public class LiveProjectionOnProducts : RavenTestBase
    {
        [Fact]
        public void SimpleLiveProjection()
        {
            using (var documentStore = GetDocumentStore())
            {
                new ProductSkuListViewModelReport_ByArticleNumberAndName().Execute((IDocumentStore)documentStore);
                new ProductSkuListViewModelReport_ByArticleNumberAndNameTransformer().Execute((IDocumentStore)documentStore);

                using (var session = documentStore.OpenSession())
                {
                    session.Store(
                        new ProductSku()
                        {
                            ArticleNumber = "v1",
                            Name = "variant 1",
                            Packing = "packing",
                        });
                    session.Store(
                        new ProductSku()
                        {
                            ArticleNumber = "v2",
                            Name = "variant 2",
                            Packing = "packing"
                        });
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var rep = session.Query<dynamic, ProductSkuListViewModelReport_ByArticleNumberAndName>()
                        .Customize(x =>
                        {
                            x.WaitForNonStaleResultsAsOfNow();
                            ((IDocumentQuery<ProductSkuListViewModelReport>)x).OrderBy("__document_id");
                        })
                        .TransformWith<ProductSkuListViewModelReport_ByArticleNumberAndNameTransformer, ProductSkuListViewModelReport>()
                        .ToList();

                    var first = rep.FirstOrDefault();

                    Assert.Equal(first.Id, "ProductSkus/1-A");

                    Assert.Equal(first.Name, "variant 1");
                }
            }
        }

        [Fact]
        public void ComplexLiveProjection()
        {
            using (var documentStore = GetDocumentStore())
            {
                new ProductDetailsReport_ByProductId().Execute((IDocumentStore)documentStore);
                new ProductDetailsReport_Transformer().Execute((IDocumentStore)documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var product = new Product()
                    {
                        Name = "product 1",
                        Variants = new List<ProductSku>()
                                {
                                    new ProductSku()
                                        {
                                            ArticleNumber = "v1",
                                            Name = "variant 1",
                                            Packing = "packing"
                                        },
                                    new ProductSku()
                                        {
                                            ArticleNumber = "v2",
                                            Name = "variant 2",
                                            Packing = "packing"
                                        }
                                }
                    };

                    session.Store(product);
                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var rep = session.Query<dynamic, ProductDetailsReport_ByProductId>()
                        .Customize(x => x.WaitForNonStaleResultsAsOfNow())
                        .TransformWith<ProductDetailsReport_Transformer, ProductDetailsReport>()
                        .ToList();

                    var first = rep.FirstOrDefault();

                    Assert.Equal(first.Name, "product 1");
                    Assert.Equal(first.Id, "products/1-A");
                    Assert.Equal(first.Variants[0].Name, "variant 1");
                }
            }
        }

        private class ProductSkuListViewModelReport_ByArticleNumberAndName : AbstractIndexCreationTask<ProductSku>
        {
            public ProductSkuListViewModelReport_ByArticleNumberAndName()
            {
                Map = products => from product in products
                                  select new
                                  {
                                      Id = product.Id,
                                      ArticleNumber = product.ArticleNumber,
                                      Name = product.Name
                                  };

                Indexes = new Dictionary<Expression<Func<ProductSku, object>>, FieldIndexing>()
                    {
                        { e=>e.ArticleNumber, FieldIndexing.Search},
                        { e=>e.Name, FieldIndexing.Search}
                    };
            }
        }

        private class ProductSkuListViewModelReport_ByArticleNumberAndNameTransformer : AbstractTransformerCreationTask<ProductSku>
        {
            public ProductSkuListViewModelReport_ByArticleNumberAndNameTransformer()
            {
                TransformResults = results =>
                                   from result in results
                                   let product = LoadDocument<ProductSku>(result.Id)
                                   let stock = LoadDocument<ProductSku>(result.Id)
                                   select new
                                   {
                                       result.Id,
                                       result.ArticleNumber,
                                       result.Name,
                                       product.Packing,
                                       stock.QuantityInWarehouse
                                   };
            }
        }

        private class ProductSkuListViewModelReport
        {
            public string Id { get; set; }

            public string ArticleNumber { get; set; }

            public string Name { get; set; }

            public string Packing { get; set; }

            public int QuantityInWarehouse { get; set; }
        }

        private class Product
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public ICollection<ProductSku> Variants { get; set; }
        }

        private class ProductSku
        {
            public string Id { get; set; }

            public string ArticleNumber { get; set; }

            public string Name { get; set; }

            public string Packing { get; set; }

            public int QuantityInWarehouse { get; set; }
        }

        private class ProductDetailsReport
        {
            public string Id { get; set; }

            public string Name { get; set; }

            public IList<ProductVariant> Variants { get; set; }
        }

        private class ProductVariant
        {
            public string ArticleNumber { get; set; }

            public string Name { get; set; }

            public string Packing { get; set; }

            public bool IsInStock { get; set; }
        }

        private class ProductDetailsReport_ByProductId : AbstractIndexCreationTask<Product, ProductDetailsReport>
        {
            public ProductDetailsReport_ByProductId()
            {
                Map = products => from product in products
                                  select new
                                  {
                                      ProductId = product.Id,
                                  };
            }
        }

        private class ProductDetailsReport_Transformer : AbstractTransformerCreationTask<Product>
        {
            public ProductDetailsReport_Transformer()
            {
                TransformResults = results =>
                                   from result in results
                                   let product = LoadDocument<Product>(result.Id)
                                   let variants = product.Variants
                                   select new
                                   {
                                       result.Id,
                                       Name = product.Name,
                                       Variants = variants.Select(x => new
                                       {
                                           x.ArticleNumber,
                                           x.Name,
                                           x.Packing,
                                           IsInStock = x.QuantityInWarehouse > 0
                                       })
                                   };
            }
        }
    }
}
