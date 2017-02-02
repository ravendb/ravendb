// -----------------------------------------------------------------------
//  <copyright file="NestingTransformers.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.NewClient.Client.Indexes;
using Xunit;

namespace SlowTests.Tests.ResultsTransformer
{
    public class NestingTransformers : RavenNewTestBase
    {
        private class Product
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class ProductTransformer : AbstractTransformerCreationTask<Product>
        {
            public class Result
            {
                public string Name { get; set; }
            }
            public ProductTransformer()
            {
                TransformResults = products => from doc in products
                                               select new
                                               {
                                                   Name = doc.Name.ToUpper()
                                               };
            }
        }

        private class ProductTransformer2 : AbstractTransformerCreationTask<Product>
        {
            public class Result
            {
                public string Name { get; set; }
            }
            public ProductTransformer2()
            {
                TransformResults = products => from doc in products
                                               select new
                                               {
                                                   Name = doc.Name.Reverse()
                                               };
            }
        }

        private class CallAnotherTransformerPerItem : AbstractTransformerCreationTask<Product>
        {
            public class Result
            {
                public Product Product { get; set; }
                public dynamic AnotherResult { get; set; }
            }

            public CallAnotherTransformerPerItem()
            {
                TransformResults = products => from doc in products
                                               select new
                                               {
                                                   Product = doc,
                                                   AnotherResult = TransformWith(Parameter("transformer").Value<string>(), doc)
                                               };
            }
        }

        private class CallAnotherTransformerPerAllItems : AbstractTransformerCreationTask<Product>
        {
            public CallAnotherTransformerPerAllItems()
            {
                TransformResults = products => from doc in products
                                               from result in TransformWith(Parameter("transformer").Value<string>(), doc)
                                               select result;
            }
        }

        private class CallMultipleTransformerPerAllItems : AbstractTransformerCreationTask<Product>
        {
            public CallMultipleTransformerPerAllItems()
            {
                TransformResults = products => from doc in products
                                               from result in TransformWith(Parameter("transformers").Value<string>().Split(';'), doc)
                                               select result;
            }
        }


        [Fact]
        public void CanCallMultipleTransformers()
        {
            using (var store = GetDocumentStore())
            {
                new ProductTransformer().Execute(store);
                new ProductTransformer2().Execute(store);
                new CallMultipleTransformerPerAllItems().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Product() { Id = "products/1", Name = "Irrelevant" });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var result = session.Load<CallMultipleTransformerPerAllItems, ProductTransformer.Result>("products/1",
                        configure => configure.AddTransformerParameter("transformers", "ProductTransformer;ProductTransformer2"));
                    Assert.Equal("TNAVELERRI", result.Name);
                }
            }
        }

        [Fact]
        public void CanCallTransformerPerItem()
        {
            using (var store = GetDocumentStore())
            {
                new ProductTransformer().Execute(store);
                new CallAnotherTransformerPerItem().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Product() { Id = "products/1", Name = "Irrelevant" });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var result = session.Load<CallAnotherTransformerPerItem, CallAnotherTransformerPerItem.Result>("products/1",
                        configure => configure.AddTransformerParameter("transformer", "ProductTransformer"));
                    Assert.Equal("IRRELEVANT", (string)result.AnotherResult[0].Name);
                }
            }

        }

        [Fact]
        public void CanCallTransformerAllItem()
        {
            using (var store = GetDocumentStore())
            {
                new ProductTransformer().Execute(store);
                new CallAnotherTransformerPerAllItems().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new Product() { Id = "products/1", Name = "Irrelevant" });
                    session.SaveChanges();
                }
                using (var session = store.OpenSession())
                {
                    var result = session.Load<CallAnotherTransformerPerAllItems, ProductTransformer.Result>("products/1",
                        configure => configure.AddTransformerParameter("transformer", "ProductTransformer"));
                    Assert.Equal("IRRELEVANT", result.Name);
                }
            }
        }
    }
}
