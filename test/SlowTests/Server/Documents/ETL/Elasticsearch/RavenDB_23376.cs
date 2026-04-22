 using System;
 using System.Collections.Generic;
 using System.Linq;
 using System.Text.Json.Serialization;
 using System.Threading.Tasks;
 using Elastic.Clients.Elasticsearch;
 using Orders;
 using Raven.Client.Documents.Indexes;
 using Raven.Client.Documents.Operations.ETL.ElasticSearch;
 using Tests.Infrastructure;
 using Xunit;

 namespace SlowTests.Server.Documents.ETL.ElasticSearch
{
     public class RavenDB_23376 : ElasticSearchEtlTestBase
     {
         public RavenDB_23376(ITestOutputHelper output) : base(output)
         {
         }

         protected string ProductsByCategoriesIndexName => $"ProductsByCategories{IndexSuffix}".ToLower();

         protected List<ElasticSearchIndex> ProductsByCategoriesIndex => new()
         {
             new ElasticSearchIndex
             {
                 IndexName = ProductsByCategoriesIndexName,
                 DocumentIdProperty = "Id"
             }
         };

         [RequiresElasticSearchRetryFact]
         public async Task ShouldPropagateDeletionsOfArtificialDocuments_BasicScenario()
         {
             using (var store = GetDocumentStore())
             using (GetElasticClient(out var client))
             {
                 await new ProductsByCategory().ExecuteAsync(store);

                 var etlScript = @"
 loadToProductsByCategories" + IndexSuffix + @"({
     Id: id(this),
     Category: this.Category,
     TotalPrice: this.TotalPrice,
     Count: this.Count
 });
 ";

                 var config = SetupElasticEtl(store, etlScript, ProductsByCategoriesIndex, new[] { "ProductsByCategories" });
                 var etlDone = Etl.WaitForEtlToComplete(store);

                 using (var session = store.OpenAsyncSession())
                 {
                     await session.StoreAsync(new Product { Name = "Product1", Category = "Electronics", PricePerUnit = 100 });
                     await session.StoreAsync(new Product { Name = "Product2", Category = "Electronics", PricePerUnit = 200 });
                     await session.StoreAsync(new Product { Name = "Product3", Category = "Books", PricePerUnit = 50 });
                     await session.SaveChangesAsync();
                 }

                 await Indexes.WaitForIndexingAsync(store);

                 using (var session = store.OpenSession())
                 {
                     WaitForValue(() => session.Query<ProductsByCategories>().Count(), 2, interval: 500);
                 }

                 await AssertEtlDoneAsync(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

                 await client.Indices.RefreshAsync(ProductsByCategoriesIndexName);

                 var count = await client.CountAsync<object>(descriptor => descriptor.Indices(Indices.Index(ProductsByCategoriesIndexName)));
                 Assert.Equal(2, count.Count);

                 var searchResponse = await client.SearchAsync<ProductsByCategories>(s => s
                     .Indices(ProductsByCategoriesIndexName)
                     .Size(10));

                 Assert.True(searchResponse.IsValidResponse);
                 Assert.Equal(2, searchResponse.Documents.Count);

                 Assert.Contains(searchResponse.Documents, x => x.Category == "Electronics" && x.TotalPrice == 300);
                 Assert.Contains(searchResponse.Documents, x => x.Category == "Books" && x.TotalPrice == 50);

                 etlDone.Reset();

                 using (var session = store.OpenAsyncSession())
                 {
                     session.Delete("products/1-A");
                     session.Delete("products/2-A");
                     session.Delete("products/3-A");
                     await session.SaveChangesAsync();
                 }

                 await Indexes.WaitForIndexingAsync(store);

                 using (var session = store.OpenSession())
                 {
                     WaitForValue(() => session.Query<ProductsByCategories>().Count(), 0, interval: 500);
                 }

                 await AssertEtlDoneAsync(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

                 await client.Indices.RefreshAsync(ProductsByCategoriesIndexName);

                 var countAfterDelete = await client.CountAsync<object>(descriptor => descriptor.Indices(Indices.Index(ProductsByCategoriesIndexName)));
                 Assert.Equal(0, countAfterDelete.Count);
             }
         }

         [RequiresElasticSearchRetryFact]
         public async Task ShouldPropagateDeletionsOfArtificialDocuments_IndexUpdateScenario()
         {
             using (var store = GetDocumentStore())
             using (GetElasticClient(out var client))
             {
                 await new ProductsByCategory().ExecuteAsync(store);

                 using (var session = store.OpenAsyncSession())
                 {
                     await session.StoreAsync(new Product { Name = "Product1", Category = "Electronics", PricePerUnit = 100 });
                     await session.StoreAsync(new Product { Name = "Product2", Category = "Electronics", PricePerUnit = 200 });
                     await session.StoreAsync(new Product { Name = "Product3", Category = "Books", PricePerUnit = 50 });
                     await session.SaveChangesAsync();
                 }

                 await Indexes.WaitForIndexingAsync(store);

                 using (var session = store.OpenSession())
                 {
                     WaitForValue(() => session.Query<ProductsByCategories>().Count(), 2, interval: 500);
                 }

                 var etlScript = @"
 loadToProductsByCategories" + IndexSuffix + @"({
     Id: id(this),
     Category: this.Category,
     TotalPrice: this.TotalPrice,
     Count: this.Count
 });
 ";

                 var config = SetupElasticEtl(store, etlScript, ProductsByCategoriesIndex, new[] { "ProductsByCategories" });
                 var etlDone = Etl.WaitForEtlToComplete(store);

                 await AssertEtlDoneAsync(etlDone, TimeSpan.FromMinutes(1), store.Database, config);

                 await client.Indices.RefreshAsync(ProductsByCategoriesIndexName);

                 var count = await client.CountAsync<object>(descriptor => descriptor.Indices(Indices.Index(ProductsByCategoriesIndexName)));
                 Assert.Equal(2, count.Count);

                 await new ProductsByCategoryUpdated().ExecuteAsync(store);

                 await Indexes.WaitForIndexingAsync(store);

                 using (var session = store.OpenSession())
                 {
                     WaitForValue(() => session.Query<ProductsByCategories>().Count(), 2, 0, interval: 500);
                 }

                 // poll destination directly - the ETL may need multiple batches to process both
                 // the new artificial docs (inserts) and old artificial doc tombstones (deletes)
                 var countAfterUpdate = await WaitForValueAsync(async () =>
                 {
                     await client.Indices.RefreshAsync(ProductsByCategoriesIndexName);
                     var count = await client.CountAsync<object>(descriptor => descriptor.Indices(Indices.Index(ProductsByCategoriesIndexName)));
                     return (int)count.Count;
                 }, 2, timeout: 60_000, interval: 500);
                 
                 Assert.Equal(2, countAfterUpdate);

                 var searchResponse = await client.SearchAsync<ProductsByCategories>(s => s
                     .Indices(ProductsByCategoriesIndexName)
                     .Size(10));

                 Assert.True(searchResponse.IsValidResponse);
                 Assert.Equal(2, searchResponse.Documents.Count);
                 Assert.Contains(searchResponse.Documents, x => x.Category == "Electronics");
                 Assert.Contains(searchResponse.Documents, x => x.Category == "Books");
             }
         }

         private class ProductsByCategories
         {
             [JsonPropertyName("Category")]
             public string Category { get; set; }

             [JsonPropertyName("TotalPrice")]
             public decimal TotalPrice { get; set; }

             [JsonPropertyName("Count")]
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
