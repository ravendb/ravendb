using System.Collections.Generic;
using System.Linq;
using FastTests;
using Raven.Client;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_22747 : RavenTestBase
    {
        public RavenDB_22747(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Recurse_Visits_Nested_Objects_With_Identical_Content(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new Posts_ByNoteContent();
                index.Execute(store);

                StorePostWithDuplicateNotes(store);

                Indexes.WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                var stats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));
                Assert.Equal(4, stats.EntriesCount);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Recurse_Visits_Nested_Objects_With_Identical_Content_JavaScript(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new Posts_ByNoteContent_JavaScript();
                index.Execute(store);

                StorePostWithDuplicateNotes(store);

                Indexes.WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                var stats = store.Maintenance.Send(new GetIndexStatisticsOperation(index.IndexName));
                Assert.Equal(4, stats.EntriesCount);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Recurse_Terminates_On_Document_Cycle(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new Nodes_ByReachable();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Node { Name = "n1", Children = new[] { "nodes/2" } }, "nodes/1");
                    session.Store(new Node { Name = "n2", Children = new[] { "nodes/3" } }, "nodes/2");
                    session.Store(new Node { Name = "n3", Children = new[] { "nodes/1" } }, "nodes/3");
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Nodes_ByReachable.Result, Nodes_ByReachable>()
                        .ProjectInto<Nodes_ByReachable.Result>()
                        .ToList();

                    Assert.Equal(3, results.Count);
                    Assert.All(results, r => Assert.Equal(3, r.Count));
                }
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Recurse_Handles_Missing_And_Null_Nested_Values(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new Posts_ByNoteContent();
                index.Execute(store);

                StorePostsWithMissingAndNullNotes(store);

                Indexes.WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                AssertMissingAndNullNotes(store, index.IndexName);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Recurse_Handles_Missing_And_Null_Nested_Values_JavaScript(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new Posts_ByNoteContent_JavaScript();
                index.Execute(store);

                StorePostsWithMissingAndNullNotes(store);

                Indexes.WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                AssertMissingAndNullNotes(store, index.IndexName);
            }
        }

        [RavenTheory(RavenTestCategory.Indexes)]
        [RavenData(SearchEngineMode = RavenSearchEngineMode.All)]
        public void Recurse_Skips_Nonexistent_Loaded_Documents(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var index = new Nodes_ByReachable();
                index.Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Node { Name = "n1", Children = new[] { "nodes/2", "nodes/missing" } }, "nodes/1");
                    session.Store(new Node { Name = "n2", Children = new[] { "nodes/missing", null } }, "nodes/2");
                    session.Store(new Node { Name = "n3", Children = null }, "nodes/3");
                    var withoutChildren = new NodeWithoutChildren { Name = "n4" };
                    session.Store(withoutChildren, "nodes/4");
                    session.Advanced.GetMetadataFor(withoutChildren)[Constants.Documents.Metadata.Collection] = "Nodes";
                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);
                RavenTestHelper.AssertNoIndexErrors(store);

                using (var session = store.OpenSession())
                {
                    var results = session.Query<Nodes_ByReachable.Result, Nodes_ByReachable>()
                        .ProjectInto<Nodes_ByReachable.Result>()
                        .ToDictionary(x => x.Name, x => x.Count);

                    Assert.Equal(4, results.Count);
                    Assert.Equal(2, results["n1"]);
                    Assert.Equal(1, results["n2"]);
                    Assert.Equal(1, results["n3"]);
                    Assert.Equal(1, results["n4"]);
                }
            }
        }

        private static void StorePostsWithMissingAndNullNotes(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                var withoutNotes = new PostWithoutNotes { Content = "missing" };
                session.Store(withoutNotes, "posts/missing");
                session.Advanced.GetMetadataFor(withoutNotes)[Constants.Documents.Metadata.Collection] = "Posts";
                session.Store(new Post { Content = "null", Notes = null }, "posts/null");
                session.Store(new Post { Content = "empty", Notes = new List<Note>() }, "posts/empty");
                session.Store(new Post
                {
                    Content = "with-nulls",
                    Notes = new List<Note>
                    {
                        new Note { Content = "first" },
                        null,
                        new Note { Content = "second", Notes = new List<Note> { null, new Note { Content = "third" } } },
                        null
                    }
                }, "posts/with-nulls");

                session.SaveChanges();
            }
        }

        private static void AssertMissingAndNullNotes(IDocumentStore store, string indexName)
        {
            var expected = new Dictionary<string, string>
            {
                ["missing"] = "posts/missing",
                ["null"] = "posts/null",
                ["empty"] = "posts/empty",
                ["with-nulls"] = "posts/with-nulls",
                ["first"] = "posts/with-nulls",
                ["second"] = "posts/with-nulls",
                ["third"] = "posts/with-nulls"
            };

            using (var session = store.OpenSession())
            {
                foreach (var (content, id) in expected)
                {
                    var post = session.Advanced.DocumentQuery<Post>(indexName)
                        .WhereEquals("Content", content)
                        .Single();

                    Assert.Equal(id, session.Advanced.GetDocumentId(post));
                }
            }
        }

        private static void StorePostWithDuplicateNotes(IDocumentStore store)
        {
            using (var session = store.OpenSession())
            {
                session.Store(new Post
                {
                    Content = "Something",
                    Notes = new List<Note>
                    {
                        new Note { Content = "some text" },
                        new Note { Content = "other text" },
                        new Note { Content = "some text" }
                    }
                });

                session.SaveChanges();
            }
        }

        private class Post
        {
            public string Content { get; set; }
            public List<Note> Notes { get; set; }
        }

        private class PostWithoutNotes
        {
            public string Content { get; set; }
        }

        private class Note
        {
            public string Content { get; set; }
            public List<Note> Notes { get; set; }
        }

        private class Node
        {
            public string Name { get; set; }
            public string[] Children { get; set; }
        }

        private class NodeWithoutChildren
        {
            public string Name { get; set; }
        }

        private class Posts_ByNoteContent : AbstractIndexCreationTask<Post>
        {
            public Posts_ByNoteContent()
            {
                Map = posts => from post in posts
                    from note in Recurse(post, item => item.Notes)
                    select new { note.Content };
            }
        }

        private class Posts_ByNoteContent_JavaScript : AbstractJavaScriptIndexCreationTask
        {
            public Posts_ByNoteContent_JavaScript()
            {
                Maps = new HashSet<string>
                {
                    """
                    map('Posts', function (post) {
                        return recurse(post, x => x.Notes).map(function (note) {
                            return { Content: note.Content };
                        });
                    })
                    """
                };
            }
        }

        private class Nodes_ByReachable : AbstractIndexCreationTask<Node, Nodes_ByReachable.Result>
        {
            public class Result
            {
                public string Name { get; set; }
                public int Count { get; set; }
            }

            public Nodes_ByReachable()
            {
                Map = nodes => from node in nodes
                    let reachable = Recurse(node, x => LoadDocument<Node>(x.Children))
                    select new Result
                    {
                        Name = node.Name,
                        Count = reachable.Count(x => x != null)
                    };

                StoreAllFields(FieldStorage.Yes);
            }
        }
    }
}
