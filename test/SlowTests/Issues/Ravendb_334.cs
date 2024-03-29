﻿// -----------------------------------------------------------------------
//  <copyright file="RavenDB_334.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System;
using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_334 : RavenTestBase
    {
        public RavenDB_334(ITestOutputHelper output) : base(output)
        {
        }

        private class Foo
        {
            public string Id { get; set; }
            public DateTime DateTime { get; set; }
        }

        private class FooIndex : AbstractIndexCreationTask<Foo>
        {
            public class IndexedFoo
            {
                public string Id { get; set; }
                public DateTime DateTime { get; set; }
            }

            public FooIndex()
            {
                Map = foos => from f in foos select new { f.Id };

                Store(x => x.DateTime, FieldStorage.Yes);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes | RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanGetUtcFromDate(Options options)
        {
            using (var documentStore = GetDocumentStore(options))
            {
                new FooIndex().Execute(documentStore);

                using (var session = documentStore.OpenSession())
                {
                    var foo = new Foo { Id = "foos/1", DateTime = DateTime.UtcNow };

                    session.Store(foo);

                    session.SaveChanges();
                }

                using (var session = documentStore.OpenSession())
                {
                    var foo = session.Load<Foo>("foos/1");

                    var indexedFoo = session.Query<Foo, FooIndex>()
                        .Customize(c => c.WaitForNonStaleResults())
                        .Single(f => f.Id == "foos/1");
                    Assert.Equal(foo.DateTime.Kind, indexedFoo.DateTime.Kind);
                    Assert.Equal(foo.DateTime, indexedFoo.DateTime);
                }
            }
        }
    }
}
