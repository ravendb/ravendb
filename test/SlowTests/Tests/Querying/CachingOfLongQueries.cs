// -----------------------------------------------------------------------
//  <copyright file="CachingOfLongQueries.cs" company="Hibernating Rhinos LTD">
//      Copyright (c) Hibernating Rhinos LTD. All rights reserved.
//  </copyright>
// -----------------------------------------------------------------------

using System.Linq;
using System.Threading.Tasks;

using FastTests;

using Xunit;

namespace SlowTests.Tests.Querying
{
    public class CachingOfLongQueries : RavenTestBase
    {
        [Fact]
        public async Task ShouldWork()
        {
            using (var store = await GetDocumentStore())
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

        public class Item
        {
            public string Val;
        }
    }
}
