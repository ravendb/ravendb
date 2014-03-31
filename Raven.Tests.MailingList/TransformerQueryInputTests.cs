// -----------------------------------------------------------------------
//  <copyright file="TransformerQueryInputTests.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;
using Raven.Json.Linq;

namespace Raven.Tests.MailingList
{
    public class TransformerQueryInputTests : RavenTestBase
    {
        [Fact]
        public void CanCastQueryInput()
        {
            using (var store = NewDocumentStore())
            {
                new FooTransformer().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Foo { Id = "foos/1", Things = { "hello", "there" } });
                    session.SaveChanges();

                    var results = session.Load<FooTransformer, FooTransformer.Result>(
                        "foos/1",
                        configuration => configuration.AddQueryParam("take", 1)
                        );

                    Assert.Equal(1, results.Keys.Count);
                }
            }
        }

        public class Foo
        {
            public string Id { get; set; }
            public List<string> Things { get; set; }

            public Foo()
            {
                Things = new List<string>();
            }
        }

        public class FooTransformer : AbstractTransformerCreationTask<Foo>
        {
            public class Result
            {
                public List<string> Keys { get; set; }
            }

            public FooTransformer()
            {
                TransformResults = foos => from foo in foos
                                           select new
                                           {
                                               Keys = foo.Things.Take(Query("take").Value<int>())
                                           };
            }
        }
    }
}