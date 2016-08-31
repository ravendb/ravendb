// -----------------------------------------------------------------------
//  <copyright file="QueryingIntArray.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Abstractions.Indexing;
using Raven.Client;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.MailingList
{
    public class QueryingIntArray : RavenTestBase
    {
        [Fact]
        public void Test()
        {
            using (var store = GetDocumentStore())
            {
                new FooIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new FooDocument
                    {
                        Name = "Test 1",
                        Resolutions = new[] { 1, 3, 5, 7, 9 }
                    });
                    session.Store(new FooDocument
                    {
                        Name = "Test 2",
                        Resolutions = new[] { 5, 7, 9, 11, 13 }
                    });

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<IndexEntry, FooIndex>()
                        .Customize(customization => customization.WaitForNonStaleResultsAsOfNow())
                        .Search(o => o.Name, "Test")
                        .Where(o => o.Resolutions.Any(x => x >= 5 && x <= 9))
                        .ProjectFromIndexFieldsInto<IndexEntry>()
                        .ToList();

                    Assert.Equal(2, results.Count);
                }
            }
        }

        private class FooDocument
        {
            public string Name { get; set; }
            public int[] Resolutions { get; set; }
        }

        private class IndexEntry
        {
            public string Name { get; set; }
            public int[] Resolutions { get; set; }
        }

        private class FooIndex : AbstractIndexCreationTask<FooDocument, IndexEntry>
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
}
