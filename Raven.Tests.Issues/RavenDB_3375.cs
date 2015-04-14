
using System;
using System.Linq;
using System.Threading;
using Raven.Client;
using Raven.Client.Embedded;
using Raven.Client.Indexes;
using Xunit;

namespace Bug
{
    public class QueryEscapeTest
    {
        public class Post
        {
            public int Id { get; set; }
            public string[] Tags { get; set; }
        }

        public class TagsIndex : AbstractIndexCreationTask<Post>
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
            using (var store = new EmbeddableDocumentStore())
            {
                store.Configuration.RunInMemory = true;
                store.Initialize();
                store.DatabaseCommands.PutIndex("TagsIndex", new TagsIndex().CreateIndexDefinition());

                using (var session = store.OpenSession())
                {
                    session.Store(new Post
                    {
                        Id = 1,
                        Tags = new[] { "NoSpace:1", "Space :2"}
                    });
                    session.SaveChanges();
                }

                WaitForIndexes(store);

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

        static void WaitForIndexes(IDocumentStore store)
        {
            do
            {
                var stats = store.DatabaseCommands.GetStatistics();
                if (stats.StaleIndexes.Length == 0) return;
                Thread.Sleep(100);
            } while (true);
        }
    }
}

