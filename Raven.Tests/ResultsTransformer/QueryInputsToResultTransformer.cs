using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.ResultsTransformer
{
    public class QueryInputsToResultTransformer : RavenTest
    {
        public class Product
        {
            public string Id { get; set; }
            public string Name { get; set; }
			public string CategoryId { get; set; }
        }

		public class Category
		{
			public string Id { get; set; }
			public string Name { get; set; }
		}

        public class ProductWithQueryInput: AbstractTransformerCreationTask<Product>
        {
            public class Result
            {
                public string ProductId { get; set; }
                public string ProductName { get; set; }
                public string Input { get; set; }
            }
            public ProductWithQueryInput()
            {
                TransformResults = docs => from product in docs
                                             select new
                                             {
                                                 ProductId = product.Id,
                                                 ProductName = product.Name,
                                                 Input = Query("input")
                                             };
            }
        }

		public class ProductWithQueryInputAndInclude : AbstractTransformerCreationTask<Product>
		{
			public class Result
			{
				public string ProductId { get; set; }
				public string ProductName { get; set; }
				public string CategoryId { get; set; }
			}
			public ProductWithQueryInputAndInclude()
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
            using (var store = NewRemoteDocumentStore())
            {
                new ProductWithQueryInput().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Product() { Id="products/1", Name = "Irrelevant"});
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var result = session.Load<ProductWithQueryInput, ProductWithQueryInput.Result>("products/1", 
                        configure => configure.AddQueryParam("input", "Foo"));
                    Assert.Equal("Foo", result.Input);
                }
            }
            
        }


        [Fact]
        public void CanUseResultsTransformerWithQueryOnLoadWithRemoteClient()
        {
            using (var store = NewRemoteDocumentStore())
            {
                new ProductWithQueryInput().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Product() { Id = "products/1", Name = "Irrelevant" });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var result = session.Load<ProductWithQueryInput, ProductWithQueryInput.Result>("products/1", 
                        configure => configure.AddQueryParam("input", "Foo"));
                    Assert.Equal("Foo", result.Input);
                }
            }

        }

        [Fact]
        public void CanUseResultsTransformerWithQueryWithRemoteDatabase()
        {
            using (var store = NewRemoteDocumentStore())
            {
                new ProductWithQueryInput().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Product() { Name = "Irrelevant" });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var result = session.Query<Product>()
                                .Customize(x => x.WaitForNonStaleResults())
                                .TransformWith<ProductWithQueryInput, ProductWithQueryInput.Result>()
                                .AddQueryInput("input", "Foo")
                                .Single();

                    Assert.Equal("Foo", result.Input);

                }
            }
        }

		[Fact]
		public void CanUseResultTransformerToLoadValueOnNonStoreFieldUsingQuery()
		{
			using (var store = NewRemoteDocumentStore())
			{
				new ProductWithQueryInput().Execute(store);
				using (var session = store.OpenSession())
				{
					session.Store(new Product() { Name = "Irrelevant" });
					session.SaveChanges();
				}
				using (var session = store.OpenSession())
				{
					var result = session.Query<Product>()
								.Customize(x => x.WaitForNonStaleResults())
								.TransformWith<ProductWithQueryInput, ProductWithQueryInput.Result>()
								.AddQueryInput("input", "Foo")
								.Single();

					Assert.Equal("Irrelevant", result.ProductName);

				}
			}
		}

        [Fact]
        public void CanUseResultsTransformerWithQuery()
        {
            using (var store = NewDocumentStore())
            {
               new ProductWithQueryInput().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Product() { Name = "Irrelevant" });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var result = session.Query<Product>()
                                .Customize(x=> x.WaitForNonStaleResults())
                                .TransformWith<ProductWithQueryInput, ProductWithQueryInput.Result>()
                                .AddQueryInput("input", "Foo")
                                .Single();

                    Assert.Equal("Foo", result.Input);

                }
            }
        }

		[Fact]
		public void CanUseResultsTransformerWithInclude()
		{
			using (var store = NewDocumentStore())
			{
				new ProductWithQueryInputAndInclude().Execute(store);
				using (var session = store.OpenSession())
				{
					session.Store(new Product { Name = "Irrelevant", CategoryId = "Category/1"});
					session.Store(new Category{Id = "Category/1", Name = "don't know"});
					session.SaveChanges();
				}
				using (var session = store.OpenSession())
				{
					var result = session.Query<Product>()
								.Customize(x => x.WaitForNonStaleResults())
								.TransformWith<ProductWithQueryInputAndInclude, ProductWithQueryInputAndInclude.Result>()
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
