using System.Collections.Generic;
using System.Linq;
using Lucene.Net.Analysis.Standard;
using Raven.Abstractions.Data;
using Raven.Abstractions.Indexing;
using Raven.Client.Bundles.MoreLikeThis;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDB_4461 : RavenTestBase
    {
        [Fact]
        public void AdditionalQueryFiltersResults()
        {
            using (var store = NewDocumentStore())
            {
                store.ExecuteIndex(new Posts_ByPostCategory());

                using (var session = store.OpenSession())
                {
                    var dataQueriedFor = new MockPost { Id = "posts/123", Body = "This is a test. Isn't it great? I hope I pass my test!", Category = "IT" };

                    var someData = new List<MockPost>
                {
                    dataQueriedFor,
                    new MockPost { Id = "posts/234", Body = "I have a test tomorrow. I hate having a test", Category = "School"},
                    new MockPost { Id = "posts/3456", Body = "Cake is great.", Category = "Cooking" },
                    new MockPost { Id = "posts/3457", Body = "This document has the word test only once", Category = "Marketing" },
                    new MockPost { Id = "posts/3458", Body = "test", Category = "Test" },
                    new MockPost { Id = "posts/3459", Body = "test", Category = "Test" }
                };
                    someData.ForEach(session.Store);

                    session.SaveChanges();
                }

                WaitForIndexing(store);

                using (var session = store.OpenSession())
                {
                    Assert.NotEmpty(session.Advanced
                        .MoreLikeThis<MockPost, Posts_ByPostCategory>(new MoreLikeThisQuery
                        {
                            DocumentId = "posts/123",
                            Fields = new[] { "Body" }
                        }).ToList());

                    Assert.Empty(session.Advanced
                        .MoreLikeThis<MockPost, Posts_ByPostCategory>(new MoreLikeThisQuery
                        {
                            DocumentId = "posts/123",
                            Fields = new[] { "Body" },
                            AdditionalQuery = "Category:IT"
                        }).ToList());
                }
            }
        }
    }

    class MockPost
    {
        public string Id { get; set; }
        public string Body { get; set; }
        public string Category { get; set; }
    }

    class Posts_ByPostCategory : AbstractIndexCreationTask<MockPost>
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
