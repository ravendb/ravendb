using System.Collections.Generic;
using System.Linq;
using FastTests;
using Lucene.Net.Analysis.Standard;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries.MoreLikeThis;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_4461 : RavenTestBase
    {
        public RavenDB_4461(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void AdditionalQueryFiltersResults()
        {
            using (var store = GetDocumentStore())
            {
                store.ExecuteIndex(new Posts_ByPostCategory());

                using (var session = store.OpenSession())
                {
                    var dataQueriedFor = new MockPost { Id = "posts/123", Body = "This is a test. Isn't it great? I hope I pass my test!", Category = "IT" };

                    var someData = new List<MockPost>
                    {
                        dataQueriedFor,
                        new MockPost {Id = "posts/234", Body = "I have a test tomorrow. I hate having a test", Category = "School"},
                        new MockPost {Id = "posts/3456", Body = "Cake is great.", Category = "Cooking"},
                        new MockPost {Id = "posts/3457", Body = "This document has the word test only once", Category = "Marketing"},
                        new MockPost {Id = "posts/3458", Body = "test", Category = "Test"},
                        new MockPost {Id = "posts/3459", Body = "test", Category = "Test"}
                    };
                    someData.ForEach(session.Store);

                    session.SaveChanges();
                }

                Indexes.WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Query<MockPost, Posts_ByPostCategory>()
                        .MoreLikeThis(f => f.UsingDocument(x => x.Id == "posts/123").WithOptions(new MoreLikeThisOptions
                        {
                            Fields = new[] { "Body" }
                        }))
                        .ToList());

                    Assert.Empty(session.Query<MockPost, Posts_ByPostCategory>()
                        .MoreLikeThis(f => f.UsingDocument(x => x.Id == "posts/123").WithOptions(new MoreLikeThisOptions
                        {
                            Fields = new[] { "Body" }
                        }))
                        .Where(x => x.Category == "IT")
                        .ToList());
                }
            }
        }

        private class MockPost
        {
            public string Id { get; set; }
            public string Body { get; set; }
            public string Category { get; set; }
        }

        private class Posts_ByPostCategory : AbstractIndexCreationTask<MockPost>
        {
            public Posts_ByPostCategory()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Body,
                                  doc.Category
                              };

                Stores.Add(x => x.Body, FieldStorage.Yes);
                Analyzers.Add(x => x.Body, typeof(StandardAnalyzer).FullName);
            }
        }
    }
}
