using System.Collections.Generic;
using System.Linq;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15450 : RavenTestBase
    {
        public RavenDB_15450(ITestOutputHelper output) : base(output)
        {
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void CanFilterAndProjectSubCollectionData(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var session = store.OpenSession())
                {
                    var document = new Document("doc-id", new List<ChildDocument>
                {
                    new ChildDocument
                    {
                        Id = "a"
                    },
                    new ChildDocument
                    {
                        Id = "b"
                    },
                    new ChildDocument
                    {
                        Id = "c"
                    },
                });
                    session.Store(document);
                    session.SaveChanges();
                }

                var doFilter = true;
                var filter = new Dictionary<string, object>();
                filter.Add("b", null);
                using (var session = store.OpenSession())
                {
                    var queryable = session.Query<Document>();
                    var projections = from s in queryable
                                      let source = s.Children
                                      select new Projection
                                      {
                                          Id = s.Id,
                                          Matched = source
                                              .Where(x => !doFilter || filter.ContainsKey(x.Id))
                                              .ToList()
                                      };

                    var result = projections.SingleOrDefault();

                    Assert.NotNull(result);
                    Assert.Equal("doc-id", result.Id);
                    Assert.Equal(1, result.Matched.Count);
                    Assert.Equal("b", result.Matched[0].Id);
                }
            }
        }

        private class Document
        {
            public Document(string id, List<ChildDocument> children)
            {
                Id = id;
                Children = children;
            }

            public string Id { get; set; }
            public List<ChildDocument> Children { get; set; }
        }

        private class ChildDocument
        {
            public string Id { get; set; }
        }

        private class Projection
        {
            public string Id { get; set; }

            public List<ChildDocument> Matched { get; set; }
        }
    }
}
