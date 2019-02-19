// -----------------------------------------------------------------------
//  <copyright file="RavenDB-4300.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_4300 : RavenTestBase
    {
        // The search text must 
        //    (1) contain a space
        //    (2) end in a backslash
        private const string SearchText = "Thing One \\";

        [Fact]
        public void Escaping_Beforehand_Works()
        {
            using (var store = GetDocumentStore())
            {
                new ExampleIndex().Execute(store);
                Populate(store);
                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query = session.Query<ExampleIndex.ReduceResult, ExampleIndex>();
                    query.Customize(c => c.WaitForNonStaleResults());
                    var results =
                        query.Search(x => x.Name , SearchText)
                            .As<Example>()
                            .ToList();

                    Assert.NotEmpty(results);
                }
            }
        }

        private static void Populate(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var example1 = new Example { Name = "Thing One \\" };
                var example2 = new Example { Name = "Thing Two" };
                var example3 = new Example { Name = "Thing Three" };
                session.Store(example1);
                session.Store(example2);
                session.Store(example3);
                session.SaveChanges();
            }
        }

        private class Example
        {
            public string Name { get; set; }
        }

        private class ExampleIndex : AbstractIndexCreationTask<Example, ExampleIndex.ReduceResult>
        {
            public ExampleIndex()
            {
                Map = venues => from v in venues
                                select new
                                {
                                    v.Name,
                                };

                Index(x => x.Name, FieldIndexing.Search);
            }

            public class ReduceResult
            {
                public string Name { get; set; }
            }
        }
    }
}