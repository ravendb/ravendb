// -----------------------------------------------------------------------
//  <copyright file="QueryingIntArray.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------
using System.Collections.Generic;
using System.Linq;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Raven.Tests.Common;

using Xunit;

namespace Raven.Tests.MailingList
{
    public class QueryingIntArray : RavenTestBase
    {
        [Fact]
        public void Test()
        {
            using (EmbeddableDocumentStore store = NewDocumentStore())
            {
                new FooIndex().Execute(store);

                using (IDocumentSession session = store.OpenSession())
                {
                    session.Store(new FooDocument
                    {
                        Name = "Test 1",
                        Resolutions = new[] {1, 3, 5, 7, 9}
                    });
                    session.Store(new FooDocument
                    {
                        Name = "Test 2",
                        Resolutions = new[] {5, 7, 9, 11, 13}
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (IDocumentSession session = store.OpenSession())
                {
                    List<IndexEntry> results = session.Query<IndexEntry, FooIndex>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .Search(o => o.Name, "Test")
                        .Where(o => o.Resolutions.Any(x => x >= 5 && x <= 9))
                        .AsProjection<IndexEntry>()
                        .ToList();

                    Assert.Equal(2, results.Count);
                }
            }
        }
    }

    public class FooDocument
    {
        public string Name { get; set; }
        public int[] Resolutions { get; set; }
    }

    public class IndexEntry
    {
        public string Name { get; set; }
        public int[] Resolutions { get; set; }
    }

    public class FooIndex : AbstractIndexCreationTask<FooDocument, IndexEntry>
    {
        public FooIndex()
        {
            Map = docs => from doc in docs
                select new IndexEntry
                {
                    Name = doc.Name,
                    Resolutions = doc.Resolutions
                };

            Indexes.Add(x => x.Name, FieldIndexing.Analyzed);
            Stores.Add(x => x.Resolutions, FieldStorage.Yes);
        }
    }
}