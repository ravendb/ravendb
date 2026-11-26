using System.Collections.Generic;
using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.MailingList
{
    public class Random : RavenTestBase
    {
        public Random(ITestOutputHelper output) : base(output)
        {
        }

        private class User
        {
            public string Id { get; set; }

            public int Age { get; set; }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanSortRandomly(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        s.Store(new User { Age = i });
                    }
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var list1 = s.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults().RandomOrdering("seed1"))
                        .ToList()
                        .Select(x => x.Age)
                        .ToList();

                    var list2 = s.Query<User>()
                        .Customize(x => x.WaitForNonStaleResults().RandomOrdering("seed2"))
                        .ToList()
                        .Select(x => x.Age)
                        .ToList();

                    Assert.False(list1.SequenceEqual(list2));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void CanSortRandomly_Dynamic(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        s.Store(new { Val = i });
                    }
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var list1 = s.Query<dynamic>()
                        .Customize(x => x.WaitForNonStaleResults().RandomOrdering("seed1"))
                        .ToList()
                        .Select(x => (int)x.Val)
                        .ToList();

                    var list2 = s.Query<dynamic>()
                        .Customize(x => x.WaitForNonStaleResults().RandomOrdering("seed2"))
                        .ToList()
                        .Select(x => (int)x.Val)
                        .ToList();

                    Assert.False(list1.SequenceEqual(list2));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void RandomOrdering_WithParameter_HonorsParameterValue(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                        s.Store(new User { Age = i });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    List<int> Order(string seed) => s.Advanced
                        .RawQuery<User>("from Users order by random($seed)")
                        .AddParameter("seed", seed)
                        .WaitForNonStaleResults()
                        .ToList()
                        .Select(x => x.Age)
                        .ToList();

                    var monday1 = Order("monday");
                    var monday2 = Order("monday");
                    var tuesday = Order("tuesday");

                    // Same parameter value must produce the same (deterministic) ordering.
                    Assert.True(monday1.SequenceEqual(monday2));

                    // Different parameter values must produce different orderings.
                    // Before the fix the seed was derived from the parameter *name* ("seed"),
                    // so both queries produced an identical ordering.
                    Assert.False(monday1.SequenceEqual(tuesday));
                }
            }
        }
    }
}
