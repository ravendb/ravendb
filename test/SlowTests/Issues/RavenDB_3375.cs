using System;
using System.Linq;
using FastTests;
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
                               select new { _ = CreateField("Tag", tag, false, false) };
            }
        }

        [Fact]
        public void CanQueryPhraseWithEscapedCharacters()
        {
            using (var store = GetDocumentStore())
            {
                store.Admin.Send(new PutIndexesOperation(new TagsIndex().CreateIndexDefinition()));

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
                    query1 = query1.WhereEquals("Tag", "NoSpace:1", false);
                    Console.WriteLine(query1.ToString()); // Tag:[["NoSpace\:1"]]
                    var posts = query1.ToArray();
                    Assert.Equal(1, posts.Length); // Passes

                    var query2 = session.Advanced.DocumentQuery<Post>("TagsIndex")
                                        .WhereEquals("Tag", "Space :2", false);
                    Console.WriteLine(query2.ToString()); // Tag:[["Space \:2"]]
                    var posts2 = query2.ToArray();
                    Assert.Equal(1, posts2.Length); // Fails

                }
            }
        }
    }
}