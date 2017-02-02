// -----------------------------------------------------------------------
//  <copyright file="CachingOfLongQueries.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;

using FastTests;

using Xunit;

namespace SlowTests.Tests.Querying
{
    public class CachingOfLongQueries : RavenNewTestBase
    {
        [Fact]
        public void ShouldWork()
        {
            using (var store = GetDocumentStore())
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
