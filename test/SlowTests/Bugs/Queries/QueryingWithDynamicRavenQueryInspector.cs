using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Xunit;
using Xunit.Abstractions;
using Tests.Infrastructure;

namespace SlowTests.Bugs.Queries
{
    public class QueryingWithDynamicRavenQueryInspector : RavenTestBase
    {
        public QueryingWithDynamicRavenQueryInspector(ITestOutputHelper output) : base(output)
        {
        }


        [RavenTheory(RavenTestCategory.Querying)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void CanInitializeDynamicRavenQueryInspector(Options options)
        {
            var blogOne = new Blog
            {
                Title = "one",
                Category = "Ravens"
            };
            var blogTwo = new Blog
            {
                Title = "two",
                Category = "Rhinos"
            };
            var blogThree = new Blog
            {
                Title = "three",
                Category = "Rhinos"
            };
            
            using (var store = GetDocumentStore(options))
            {
                using (var s = store.OpenSession())
                {
                    s.Store(blogOne);
                    s.Store(blogTwo);
                    s.Store(blogThree);
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var blogs = s.Query<Blog>().AsQueryable();

                    var blogQuery = from b in blogs
                                    where b.Title == "two"
                                    select b;

                    var results = blogs.Provider.CreateQuery(blogQuery.Expression).As<Blog>().ToArray();
                    Assert.True(results.Any(x => x.Title == "two"));
                }
            }            
        }

        private class Blog
        {
            public string Title { get; set; }
            public int SortWeight { get; set; }
            public string Category { get; set; }
        }
    }
}
