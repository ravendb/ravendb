using System.Collections.Generic;
using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16656 : RavenTestBase
    {
        public RavenDB_16656(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldIncludeReferenceIndexingDetails()
        {
            using (var store = GetDocumentStore())
            {
                var index = new Products_ByCategory();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Category { Id = "categories/0", Name = "foo"});
                    session.Store(new Category { Id = "categories/1", Name = "bar"});

                    for (int i = 0; i < 200; i++)
                    {
                        session.Store(new Product { Category = $"categories/{i % 2}"});
                    }

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Category { Id = "categories/1", Name = "baz" });

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                var indexInstance = GetDatabase(store.Database).Result.IndexStore.GetIndex(index.IndexName);

                var stats = indexInstance.GetIndexingPerformance();

                 var referenceRunDetails = stats.SelectMany(x => x.Details.Operations.Select(y => y.ReferenceDetails)).Where(x => x != null && x.ReferenceAttempts > 0).ToList();

                 Assert.Equal(1, referenceRunDetails.Count);
                 Assert.Equal(100, referenceRunDetails[0].ReferenceAttempts);
                 Assert.Equal(100, referenceRunDetails[0].ReferenceSuccesses);
                 Assert.Equal(0, referenceRunDetails[0].ReferenceErrors);
            }
        }

        private class Products_ByCategory : AbstractIndexCreationTask<Product>
        {
            public Products_ByCategory()
            {
                Map = products => from product in products
                    let category = LoadDocument<Category>(product.Category)
                    select new
                    {
                        CategoryId = category.Name
                    };
            }
        }
    }
}
