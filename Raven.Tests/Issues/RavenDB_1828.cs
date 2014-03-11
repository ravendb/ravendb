// -----------------------------------------------------------------------
//  <copyright file="RavenDB_1828.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using Raven.Tests.ResultsTransformer;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_1828 : RavenTest
    {
        [Fact]
        public void CanUseResultsTransformerByName()
        {
            using (var store = NewDocumentStore())
            {
                new QueryInputsToResultTransformer.ProductWithQueryInput().Execute(store);
                using (var session = store.OpenSession())
                {
                    session.Store(new QueryInputsToResultTransformer.Product() { Name = "Irrelevant" });
                    session.SaveChanges();
                }

                var t = new QueryInputsToResultTransformer.ProductWithQueryInput();
                Console.WriteLine(t.TransformerName);
                using (var session = store.OpenSession())
                {
                    var result = session.Query<QueryInputsToResultTransformer.Product>()
                                .Customize(x => x.WaitForNonStaleResults())
                                .TransformWith<QueryInputsToResultTransformer.ProductWithQueryInput.Result>("ProductWithQueryInput")
                                .AddQueryInput("input", "Foo")
                                .Single();

                    Assert.Equal("Foo", result.Input);

                }
            }
        } 
    }
}