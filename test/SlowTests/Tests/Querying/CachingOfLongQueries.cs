// -----------------------------------------------------------------------
//  <copyright file="CachingOfLongQueries.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using Xunit.Abstractions;

using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Tests.Querying
{
    public class CachingOfLongQueries : RavenTestBase
    {
        public CachingOfLongQueries(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.Lucene)]
        public void ShouldWork(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var val = new string('a', 2048);
                using (var session = store.OpenSession())
                {
                    session.Store(new Item { Val = val });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Query<Item>()
                        .Where(x => x.Val == val)
                        .ToList());

                    val = new string('b', 2048);

                    Assert.Empty(session.Query<Item>()
                        .Where(x => x.Val == val)
                        .ToList());
                }
            }
        }

        private class Item
        {
            public string Val;
        }
    }
}
