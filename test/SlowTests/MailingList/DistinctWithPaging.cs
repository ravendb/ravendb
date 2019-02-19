// -----------------------------------------------------------------------
//  <copyright file="DistinctWithPaging.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.MailingList
{
    public class DistinctWithPaging : RavenTestBase
    {
        private class Item
        {
            public int Val { get; set; }
        }

        private class ItemIndex : AbstractIndexCreationTask<Item>
        {
            public ItemIndex()
            {
                Map = items =>
                      from item in items
                      select new { item.Val };
                Store(x => x.Val, FieldStorage.Yes);
            }
        }

        [Fact]
        public void CanWorkProperly()
        {
            using (var store = GetDocumentStore())
            {
                new ItemIndex().Execute(store);
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 25; i++)
                    {
                        session.Store(new Item { Val = i + 1 });
                        session.Store(new Item { Val = i + 1 });
                    }
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    QueryStatistics stats;
                    var results = session.Query<Item, ItemIndex>()
                                         .Statistics(out stats)
                                         .Customize(x => x.WaitForNonStaleResults())
                                         .OrderBy(t => t.Val)
                                         .Select(t => t.Val)
                                         .Distinct()
                                         .Skip(0)
                                         .Take(10)
                                         .ToList();

                    Assert.Equal(Enumerable.Range(1, 10), results);

                    var skippedResults = stats.SkippedResults;
                    results = session.Query<Item, ItemIndex>()
                                        .Statistics(out stats)
                                        .OrderBy(t => t.Val)
                                        .Select(t => t.Val)
                                        .Distinct()
                                        .Skip(results.Count + skippedResults)
                                        .Take(10)
                                        .ToList();

                    Assert.Equal(Enumerable.Range(11, 10), results);
                }
            }
        }
    }
}
