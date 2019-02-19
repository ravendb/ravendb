// -----------------------------------------------------------------------
//  <copyright file="Tobias2.cs" company="Hibernating Rhinos LTD">
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
    public class SortOnNullableTests : RavenTestBase
    {
        private readonly SortOnNullableEntity[] _data = new[]
        {
            new SortOnNullableEntity {Text = "fail", Num = null},
            new SortOnNullableEntity {Text = "foo", Num = 2},
            new SortOnNullableEntity {Text = "boo", Num = 1}
        };

        [Fact]
        public void SortOnNullable()
        {
            using (var store = GetDocumentStore())
            {
                new SortOnNullableEntity_Search().Execute(store);
                using (var session = store.OpenSession())
                {
                    foreach (var d in _data) session.Store(d);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    QueryStatistics stats = null;
                    var tst = session.Advanced.DocumentQuery<SortOnNullableEntity, SortOnNullableEntity_Search>()
                        .WaitForNonStaleResults()
                        .Statistics(out stats)
                        .OrderBy(x => x.Num)
                        .ToList();

                    Assert.NotEmpty(tst);
                    Assert.Equal("fail", tst[0].Text);
                    Assert.Equal("boo", tst[1].Text);
                    Assert.Equal("foo", tst[2].Text);
                    Assert.False(stats.IsStale, "Index is stale.");
                }
            }
        }

        private class SortOnNullableEntity
        {
            public string Id { get; set; }
            public string Text { get; set; }
            public int? Num { get; set; }
        }

        private class SortOnNullableEntity_Search : AbstractIndexCreationTask<SortOnNullableEntity>
        {
            public SortOnNullableEntity_Search()
            {
                Map = docs => from d in docs
                              select new
                              {
                                  Text = d.Text,
                                  Num = d.Num
                              };

                Index(x => x.Text, FieldIndexing.Search);
                Index(x => x.Num, FieldIndexing.Default);

            }
        }
    }
}
