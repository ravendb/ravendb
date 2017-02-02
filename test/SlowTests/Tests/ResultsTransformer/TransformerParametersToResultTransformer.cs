using System.Linq;
using FastTests;
using Raven.NewClient.Client.Indexes;
using Xunit;

namespace SlowTests.Tests.ResultsTransformer
{
    public class TransformerParametersToResultTransformer : RavenNewTestBase
    {
        private class Product
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string CategoryId { get; set; }
        }

        private class Category
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class ProductWithParameter : AbstractTransformerCreationTask<Product>
        {
            public class Result
            {
                public string ProductId { get; set; }
                public string ProductName { get; set; }
                public string Input { get; set; }
            }
            public ProductWithParameter()
            {
                TransformResults = docs => from product in docs
                                           select new
                                           {
                                               ProductId = product.Id,
                                               ProductName = product.Name,
                                               Input = Parameter("input")
                                           };
            }
        }

        private class ProductWithParametersAndInclude : AbstractTransformerCreationTask<Product>
        {
            public class Result
            {
                public string ProductId { get; set; }
                public string ProductName { get; set; }
                public string CategoryId { get; set; }
            }
            public ProductWithParametersAndInclude()
            {
                TransformResults = docs => from product in docs
                                           let _ = Include(product.CategoryId)
                                           select new
                                           {
                                               ProductId = product.Id,
                                               ProductName = product.Name,
                                               product.CategoryId,
                                           };
            }
        }

        [Fact]
        public void CanUseResultsTransformerWithQueryOnLoad()
        {
            using (var store = GetDocumentStore())
            {
                new ProductWithParameter().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Product() { Id = "products/1", Name = "Irrelevant" });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var result = session.Load<ProductWithParameter, ProductWithParameter.Result>("products/1",
                        configure => configure.AddTransformerParameter("input", "Foo"));
                    Assert.Equal("Foo", result.Input);
                }
            }

        }


        [Fact]
        public void CanUseResultsTransformerWithQueryOnLoadWithRemoteClient()
        {
            using (var store = GetDocumentStore())
            {
                new ProductWithParameter().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Product() { Id = "products/1", Name = "Irrelevant" });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var result = session.Load<ProductWithParameter, ProductWithParameter.Result>("products/1",
                        configure => configure.AddTransformerParameter("input", "Foo"));
                    Assert.Equal("Foo", result.Input);
                }
            }

        }

        [Fact]
        public void CanUseResultsTransformerWithQueryWithRemoteDatabase()
        {
            using (var store = GetDocumentStore())
            {
                new ProductWithParameter().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Product() { Name = "Irrelevant" });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var result = session.Query<Product>()
                                .Customize(x => x.WaitForNonStaleResults())
                                .TransformWith<ProductWithParameter, ProductWithParameter.Result>()
                                .AddTransformerParameter("input", "Foo")
                                .Single();

                    Assert.Equal("Foo", result.Input);

                }
            }
        }

        [Fact]
        public void CanUseResultTransformerToLoadValueOnNonStoreFieldUsingQuery()
        {
            using (var store = GetDocumentStore())
            {
                new ProductWithParameter().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Product() { Name = "Irrelevant" });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var result = session.Query<Product>()
                                .Customize(x => x.WaitForNonStaleResults())
                                .TransformWith<ProductWithParameter, ProductWithParameter.Result>()
                                .AddTransformerParameter("input", "Foo")
                                .Single();

                    Assert.Equal("Irrelevant", result.ProductName);

                }
            }
        }

        [Fact]
        public void CanUseResultsTransformerWithQuery()
        {
            using (var store = GetDocumentStore())
            {
                new ProductWithParameter().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Product() { Name = "Irrelevant" });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var result = session.Query<Product>()
                                .Customize(x => x.WaitForNonStaleResults())
                                .TransformWith<ProductWithParameter, ProductWithParameter.Result>()
                                .AddTransformerParameter("input", "Foo")
                                .Single();

                    Assert.Equal("Foo", result.Input);

                }
            }
        }

        [Fact]
        public void CanUseResultsTransformerWithInclude()
        {
            using (var store = GetDocumentStore())
            {
                new ProductWithParametersAndInclude().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Product { Name = "Irrelevant", CategoryId = "Category/1" });
                    session.Store(new Category { Id = "Category/1", Name = "don't know" });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var result = session.Query<Product>()
                                .Customize(x => x.WaitForNonStaleResults())
                                .TransformWith<ProductWithParametersAndInclude, ProductWithParametersAndInclude.Result>()
                                .Single();
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.NotNull(result);
                    var category = session.Load<Category>(result.CategoryId);
                    Assert.Equal(1, session.Advanced.NumberOfRequests);
                    Assert.NotNull(category);
                }
            }
        }
    }
}
