using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Random(ITestOutputHelper output) : RavenTestBase(output)
    {
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
    }
}
