using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Server.Documents.ETL.Raven
{
    public class RavenDB_23376_Raven : RavenTestBase
    {
        public RavenDB_23376_Raven(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task ShouldPropagateDeletionsOfArtificialDocuments_BasicScenario()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                await new ProductsByCategory().ExecuteAsync(src);

                using (var session = src.OpenAsyncSession())
                {
                    await session.StoreAsync(new Product { Name = "Product1", Category = "Electronics", PricePerUnit = 100 });
                    await session.StoreAsync(new Product { Name = "Product2", Category = "Electronics", PricePerUnit = 200 });
                    await session.StoreAsync(new Product { Name = "Product3", Category = "Books", PricePerUnit = 50 });
                    await session.SaveChangesAsync();
                }

                await Indexes.WaitForIndexingAsync(src);

                using (var session = src.OpenSession())
                {
                    WaitForValue(() => session.Query<ProductsByCategories>().Count(), 2, interval: 500);
                }

                var etlDone = Etl.WaitForEtlToComplete(src);

                Etl.AddEtl(src, dest, "ProductsByCategories", script: @"loadToProductsByCategories(this);");

                Assert.True(await etlDone.WaitAsync(TimeSpan.FromMinutes(1)));

                using (var session = dest.OpenAsyncSession())
                {
                    var categorySummaries = await session.Query<ProductsByCategories>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToListAsync();

                    Assert.Equal(2, categorySummaries.Count);
                    Assert.Contains(categorySummaries, x => x.Category == "Electronics" && x.TotalPrice == 300);
                    Assert.Contains(categorySummaries, x => x.Category == "Books" && x.TotalPrice == 50);
                }

                etlDone.Reset();
                
                using (var session = src.OpenAsyncSession())
                {
                    session.Delete("products/1-A");
                    session.Delete("products/2-A");
                    session.Delete("products/3-A");
                    await session.SaveChangesAsync();
                }

                await Indexes.WaitForIndexingAsync(src);

                using (var session = src.OpenSession())
                {
                    WaitForValue(() => session.Query<ProductsByCategories>().Count(), 0, interval: 500);
                }

                await etlDone.WaitAsync(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenAsyncSession())
                {
                    var categorySummaries = await session.Query<ProductsByCategories>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToListAsync();

                    Assert.Equal(0, categorySummaries.Count);
                }
            }
        }

        [RavenFact(RavenTestCategory.Etl)]
        public async Task ShouldPropagateDeletionsOfArtificialDocuments_IndexUpdateScenario()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                await new ProductsByCategory().ExecuteAsync(src);

                using (var session = src.OpenAsyncSession())
                {
                    await session.StoreAsync(new Product { Name = "Product1", Category = "Electronics", PricePerUnit = 100 });
                    await session.StoreAsync(new Product { Name = "Product2", Category = "Electronics", PricePerUnit = 200 });
                    await session.StoreAsync(new Product { Name = "Product3", Category = "Books", PricePerUnit = 50 });
                    await session.SaveChangesAsync();
                }

                await Indexes.WaitForIndexingAsync(src);

                var etlDone = Etl.WaitForEtlToComplete(src);

                Etl.AddEtl(src, dest, "ProductsByCategories", script: @"loadToProductsByCategories(this);");

                await etlDone.WaitAsync(TimeSpan.FromMinutes(1));

                using (var session = dest.OpenAsyncSession())
                {
                    var categorySummaries = await session.Query<ProductsByCategories>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToListAsync();

                    Assert.Equal(2, categorySummaries.Count);
                }

                // Update the index definition - this will cause artificial document IDs to be regenerated
                await new ProductsByCategoryUpdated().ExecuteAsync(src);

                await Indexes.WaitForIndexingAsync(src);

                using (var session = src.OpenSession())
                {
                    WaitForValue(() => session.Query<ProductsByCategories>().Count(), 2, 0, interval: 500);
                }

                // poll destination directly - the ETL may need multiple batches to process both
                // the new artificial docs (inserts) and old artificial doc tombstones (deletes)
                WaitForValue(() =>
                {
                    using (var session = dest.OpenSession())
                    {
                        return session.Query<ProductsByCategories>().Count();
                    }
                }, 2, timeout: 60_000, interval: 500);

                using (var session = dest.OpenAsyncSession())
                {
                    var categorySummaries = await session.Query<ProductsByCategories>()
                        .Customize(x => x.WaitForNonStaleResults())
                        .ToListAsync();

                    Assert.Equal(2, categorySummaries.Count);
                    Assert.Contains(categorySummaries, x => x.Category == "Electronics");
                    Assert.Contains(categorySummaries, x => x.Category == "Books");
                }
            }
        }

        private class ProductsByCategories
        {
            public string Category { get; set; }
            public decimal TotalPrice { get; set; }
            public int Count { get; set; }
        }

        private class ProductsByCategory : AbstractIndexCreationTask<Product, ProductsByCategories>
        {
            public ProductsByCategory()
            {
                Map = products =>
                    from product in products
                    select new ProductsByCategories
                    {
                        Category = product.Category,
                        TotalPrice = product.PricePerUnit,
                        Count = 1
                    };

                Reduce = results =>
                    from result in results
                    group result by result.Category
                    into g
                    select new ProductsByCategories
                    {
                        Category = g.Key,
                        TotalPrice = g.Sum(x => x.TotalPrice),
                        Count = g.Sum(x => x.Count)
                    };

                OutputReduceToCollection = "ProductsByCategories";
            }
        }

        private class ProductsByCategoryUpdated : AbstractIndexCreationTask<Product, ProductsByCategories>
        {
            public override string IndexName => "ProductsByCategory";

            public ProductsByCategoryUpdated()
            {
                Map = products =>
                    from product in products
                    select new ProductsByCategories
                    {
                        Category = product.Category,
                        TotalPrice = product.PricePerUnit,
                        Count = 1
                    };

                Reduce = results =>
                    from result in results
                    group result by result.Category
                    into g
                    select new ProductsByCategories
                    {
                        Category = g.Key,
                        TotalPrice = g.Sum(x => x.TotalPrice),
                        Count = g.Sum(x => x.Count)
                    };

                OutputReduceToCollection = "ProductsByCategories";
                
                // to make the index different
                AdditionalSources = new Dictionary<string, string>
                {
                    { "CustomScript", "// Updated version" }
                };
            }
        }
    }
}
