using System;
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace FastTests.NewClient.ResultsTransformer
{
    public class TransformerParametersToResultTransformer : RavenTestBase
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

        public class ProductWithParameter : AbstractTransformerCreationTask<Product>
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

        public class ProductWithParametersAndInclude : AbstractTransformerCreationTask<Product>
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
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Product() { Id = "products/1", Name = "Irrelevant" });
                    session.SaveChanges();
                }
                using (var session = store.OpenNewSession())
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
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Product() { Id = "products/1", Name = "Irrelevant" });
                    session.SaveChanges();
                }
                using (var session = store.OpenNewSession())
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
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Product() { Name = "Irrelevant" });
                    session.SaveChanges();
                }
                using (var session = store.OpenNewSession())
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
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Product() { Name = "Irrelevant" });
                    session.SaveChanges();
                }
                using (var session = store.OpenNewSession())
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
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Product() { Name = "Irrelevant" });
                    session.SaveChanges();
                }
                using (var session = store.OpenNewSession())
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
                using (var session = store.OpenNewSession())
                {
                    session.Store(new Product { Name = "Irrelevant", CategoryId = "Category/1" });
                    session.Store(new Category { Id = "Category/1", Name = "don't know" });
                    session.SaveChanges();
                }
                using (var session = store.OpenNewSession())
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

        [Fact]
        public void CanCastTransformerParameter()
        {
            using (var store = GetDocumentStore())
            {
                new CastTransformer().Execute(store);

                using (var session = store.OpenNewSession())
                {
                    session.Store(new User { Id = "users/512", Name = "Tony", LastName = "Vespa"});
                    session.SaveChanges();

                    var results = session.Load<CastTransformer, CastTransformer.Result>("users/512",
                        configuration =>
                        {
                            configuration.AddTransformerParameter("int", 1);
                            configuration.AddTransformerParameter("long", 8589934592);
                            configuration.AddTransformerParameter("float", (float) 3.14);
                            configuration.AddTransformerParameter("decimal", (decimal) 0.5);
                            configuration.AddTransformerParameter("double", (double) 0.59);
                            configuration.AddTransformerParameter("bool", true);
                            configuration.AddTransformerParameter("string", "word");
                            configuration.AddTransformerParameter("datetime", new DateTime(1985, 6, 3));
                        });

                    Assert.Equal("Tony Vespa", results.Username);
                    Assert.IsType<int>(results.IntValue);
                    Assert.IsType<long>(results.LongValue);
                    Assert.IsType<float>(results.FloatValue);
                    Assert.IsType<decimal>(results.DecimalValue);
                    Assert.IsType<double>(results.DoubleValue);
                    Assert.IsType<bool>(results.BooleanValue);
                    Assert.IsType<string>(results.StringValue);
                    Assert.IsType<DateTime>(results.DateTimeValue);
                }
            }
        }
        
        private class CastTransformer : AbstractTransformerCreationTask<User>
        {
            public class Result
            {
                public string Username { get; set; }
                public int IntValue { get; set; }
                public long LongValue { get; set; }
                public float FloatValue { get; set; }
                public decimal DecimalValue { get; set; }
                public double DoubleValue { get; set; }
                public bool BooleanValue { get; set; }
                public string StringValue { get; set; }
                public DateTime DateTimeValue { get; set; }
            }

            public CastTransformer()
            {
                TransformResults = users => from user in users
                                           select new
                                           {
                                               Username = user.Name + " " + user.LastName,
                                               IntValue = Parameter("int").Value<int>(),
                                               LongValue = Parameter("long").Value<long>(),
                                               FloatValue = Parameter("float").Value<float>(),
                                               DecimalValue = Parameter("decimal").Value<decimal>(),
                                               DoubleValue = Parameter("double").Value<double>(),
                                               BooleanValue = Parameter("bool").Value<bool>(),
                                               StringValue = Parameter("string").Value<string>(),
                                               DateTimeValue = Parameter("datetime").Value<DateTime>()
                                           };
            }
        }
    }
}
