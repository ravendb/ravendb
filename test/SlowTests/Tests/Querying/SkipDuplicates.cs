using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Raven.Client.Data;
using Raven.Client.Indexes;
using Xunit;

namespace SlowTests.Tests.Querying
{
    public class SkipDuplicates : RavenTestBase
    {
        [Fact]
        public async Task WillSkipDuplicates()
        {
            using (var store = await GetDocumentStore())
            {
                store.DatabaseCommands.PutIndex(
                    "BlogPosts/PostsCountByTag",
                    new IndexDefinitionBuilder<BlogPost>()
                    {
                        Map = posts => from post in posts
                                       from tag in post.Tags
                                       select new { Tag = tag }
                    });

                using (var session = store.OpenSession())
                {
                    session.Store(new BlogPost
                    {
                        Tags = new[] { "Daniel", "Oren" }
                    });
                    session.SaveChanges();

                    WaitForIndexing(store);

                    var result = session.Query<BlogPost>("BlogPosts/PostsCountByTag").ToList();
                    Assert.Equal(1, result.Count);
                }
            }
        }

        [Fact]
        public async Task WillNotSkipDuplicates()
        {
            using (var store = await GetDocumentStore())
            {
                store.DatabaseCommands.PutIndex(
                    "BlogPosts/PostsCountByTag",
                    new IndexDefinitionBuilder<BlogPost>
                    {
                        Map = posts => from post in posts
                                       from tag in post.Tags
                                       select new { Tag = tag }
                    });

                using (var session = store.OpenSession())
                {
                    session.Store(new BlogPost
                    {
                        Tags = new[] { "Daniel", "Oren" }
                    });
                    session.SaveChanges();

                    WaitForIndexing(store);

                    var result = store.DatabaseCommands.Query("BlogPosts/PostsCountByTag", new IndexQuery { SkipDuplicateChecking = true });
                    Assert.Equal(2, result.Results.Count);
                }
            }
        }

        private class BlogPost
        {
            public string[] Tags { get; set; }
            public string Title { get; set; }
            public string BodyText { get; set; }
        }
    }
}
