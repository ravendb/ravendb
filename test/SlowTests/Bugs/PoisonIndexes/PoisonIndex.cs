using FastTests;
using Xunit;
using System.Linq;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.PoisonIndexes
{
    public class PoisonIndex : RavenTestBase
    {
        public PoisonIndex(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All, DatabaseMode = RavenDatabaseMode.All)]
        public void ShouldNotCauseFailures(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new User());
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Query<Blog>()
                           .Customize(x => x.WaitForNonStaleResults())
                           .OrderBy(x => x.Category)
                           .ToList();

                    Assert.NotEmpty(session.Query<User>()
                                           .Customize(x => x.WaitForNonStaleResults())
                                           .ToList());
                }
            }
        }

        private class Blog
        {
            public string Category { get; set; }
        }

        private class User
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string PartnerId { get; set; }
            public string Email { get; set; }
            public string[] Tags { get; set; }
            public int Age { get; set; }
            public bool Active { get; set; }
        }
    }
}
