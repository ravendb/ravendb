using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class QueryEscapeTest : RavenTestBase
    {
        private class Post
        {
            public string Id { get; set; }
            public string[] Tags { get; set; }
        }

        private class TagsIndex : AbstractIndexCreationTask<Post>
        {
            public TagsIndex()
            {
                Map = posts => from post in posts
                               from tag in post.Tags
                               select new { _ = CreateField("Tags", tag, false, false) };
            }
        }

        [Fact]
        public void CanQueryPhraseWithEscapedCharacters()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutIndexesOperation(new TagsIndex().CreateIndexDefinition()));

                using (var session = store.OpenSession())
                {
                    session.Store(new Post
                    {
                        Tags = new[] { "NoSpace:1", "Space :2" }
                    });
                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    var query1 = session.Advanced.DocumentQuery<Post>("TagsIndex");
                    query1 = query1.WhereEquals("Tags", "NoSpace:1", exact: true);
                    var posts = query1.ToArray();
                    Assert.Equal(1, posts.Length); // Passes

                    var query2 = session.Advanced.DocumentQuery<Post>("TagsIndex")
                                        .WhereEquals("Tags", "Space :2", exact: true);
                    var posts2 = query2.ToArray();
                    Assert.Equal(1, posts2.Length); // Fails
                }

                using (var session = store.OpenSession())
                {
                    var posts = session.Query<Post>("TagsIndex")
                        .Where(x => x.Tags.Contains("NoSpace:1"), exact: true)
                        .ToList();

                    Assert.Equal(1, posts.Count);
                }
            }
        }
    }
}