using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Tests.Querying
{
    public class SkipDuplicates : RavenTestBase
    {
        public SkipDuplicates(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void WillSkipDuplicates()
        {
            using (var store = GetDocumentStore())
            {
                var indexDefinition = new IndexDefinitionBuilder<BlogPost>()
                {
                    Map = posts => from post in posts
                        from tag in post.Tags
                        select new {Tag = tag}
                }.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "BlogPosts/PostsCountByTag";
                store.Maintenance.Send(new PutIndexesOperation( new [] {indexDefinition}));

                using (var session = store.OpenSession())
                {
                    session.Store(new BlogPost
                    {
                        Tags = new[] { "Daniel", "Oren" }
                    });
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

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
                var indexDefinition = new IndexDefinitionBuilder<BlogPost>
                {
                    Map = posts => from post in posts
                                   from tag in post.Tags
                                   select new { Tag = tag }
                }.ToIndexDefinition(store.Conventions);
                indexDefinition.Name = "BlogPosts/PostsCountByTag";
                store.Maintenance.Send(new PutIndexesOperation(new[] { indexDefinition }));

               

                using (var session = store.OpenSession())
                {
                    session.Store(new BlogPost
                    {
                        Tags = new[] { "Daniel", "Oren" }
                    });
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    using (var commands = store.Commands())
                    {
                        var result = commands.Query(new IndexQuery() { Query = "FROM INDEX 'BlogPosts/PostsCountByTag'", SkipDuplicateChecking = true });
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
