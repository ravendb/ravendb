using System.Linq;
using FastTests;
using Raven.NewClient.Client.Data;
using Raven.NewClient.Client.Indexes;
using Raven.NewClient.Operations.Databases.Indexes;
using Xunit;

namespace SlowTests.Tests.Querying
{
    public class SkipDuplicates : RavenNewTestBase
    {
        [Fact]
        public void WillSkipDuplicates()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexOperation(
                    "BlogPosts/PostsCountByTag",
                    new IndexDefinitionBuilder<BlogPost>()
                    {
                        Map = posts => from post in posts
                                       from tag in post.Tags
                                       select new { Tag = tag }
                    }.ToIndexDefinition(store.Conventions)));

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
        public void WillNotSkipDuplicates()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexOperation(
                    "BlogPosts/PostsCountByTag",
                    new IndexDefinitionBuilder<BlogPost>
                    {
                        Map = posts => from post in posts
                                       from tag in post.Tags
                                       select new { Tag = tag }
                    }.ToIndexDefinition(store.Conventions)));

                using (var session = store.OpenSession())
                {
                    session.Store(new BlogPost
                    {
                        Tags = new[] { "Daniel", "Oren" }
                    });
                    session.SaveChanges();

                    WaitForIndexing(store);

                    using (var commands = store.Commands())
                    {
                        var result = commands.Query("BlogPosts/PostsCountByTag", new IndexQuery(store.Conventions) { SkipDuplicateChecking = true });
                        Assert.Equal(2, result.Results.Length);
                    }
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
