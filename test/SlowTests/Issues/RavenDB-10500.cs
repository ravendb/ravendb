using System.Collections.Generic;
using System.Linq;
using FastTests;
using FastTests.Server.JavaScript;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class QueriesWithParameterCollections : RavenTestBase
    {
        public QueriesWithParameterCollections(ITestOutputHelper output) : base(output)
        {
        }

        private class Document
        {
            public string Id { get; set; }
            public string TargetId { get; set; }
            public List<Document> SubDocuments { get; set; }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void CanQueryWhenLetPresent(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var session = store.OpenSession())
                {
                    var doc = new Document
                    {
                        SubDocuments = new List<Document>
                        {
                            new Document
                            {
                                TargetId = "foo"
                            },
                            new Document
                            {
                                TargetId = "bar"
                            }
                        }
                    };

                    session.Store(doc);
                    session.SaveChanges();
                }

                var targetIds = new List<string>
                {
                    "foo"
                };
                using (var session = store.OpenSession())
                {
                    var projection =
                        from d in session.Query<Document>().Customize(x => x.WaitForNonStaleResults())
                        let temp = d.SubDocuments.FirstOrDefault(x => x != null)
                        select new
                        {
                            d.Id,
                            d.TargetId,
                            Values = d.SubDocuments
                                .Where(x => targetIds.Count == 0 || targetIds.Contains(x.TargetId))
                                .ToList()
                        };

                    var projectionResult = projection.ToList();

                    Assert.Equal(1, projectionResult.Count);
                    Assert.Equal(1, projectionResult[0].Values.Count);
                }
            }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void CanQueryWhenLetPresent_WithArrayParameter(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                using (var session = store.OpenSession())
                {
                    var doc = new Document
                    {
                        SubDocuments = new List<Document>
                        {
                            new Document
                            {
                                TargetId = "foo"
                            },
                            new Document
                            {
                                TargetId = "bar"
                            }
                        }
                    };

                    session.Store(doc);
                    session.SaveChanges();
                }

                var targetIds = new []
                {
                    "foo", "bar"
                };
                using (var session = store.OpenSession())
                {
                    var projection =
                        from d in session.Query<Document>().Customize(x => x.WaitForNonStaleResults())
                        let temp = d.SubDocuments.FirstOrDefault(x => x != null)
                        select new
                        {
                            d.Id,
                            d.TargetId,
                            Values = d.SubDocuments
                                .Where(x => targetIds.Length == 0 || targetIds.Contains(x.TargetId))
                                .ToList()

                        };

                    var projectionResult = projection.ToList();

                    Assert.Equal(1, projectionResult.Count);
                    Assert.Equal(2, projectionResult[0].Values.Count);
                }
            }
        }

    }
}
