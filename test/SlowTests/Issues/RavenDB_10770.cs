using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using FastTests.Server.JavaScript;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10770 : RavenTestBase
    {
        public RavenDB_10770(ITestOutputHelper output) : base(output)
        {
        }

        public class Document
        {
            public string Id { get; set; }
            public string TargetId { get; set; }
            public decimal TargetValue { get; set; }
            public bool Deleted { get; set; }
            public List<Document> SubDocuments { get; set; }
        }

        [Theory]
        [JavaScriptEngineClassData]
        public void ConcurrentProjectionsWithLazy(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
            {
                var doc1 = new Document
                {
                    Deleted = false,
                    SubDocuments = new List<Document>
                    {
                        new Document
                        {
                            TargetId = "id1",
                            SubDocuments = Enumerable.Range(1, 50).Select(x => new Document()).ToList()
                        },
                        new Document
                        {
                            TargetId = "id2",
                            SubDocuments = Enumerable.Range(1, 50).Select(x => new Document()).ToList()
                        }
                    }
                };

                var doc2 = new Document();
                var doc3 = new Document();

                using (var session = store.OpenSession())
                {
                    session.Store(doc1);
                    session.Store(doc2);
                    session.Store(doc3);

                    session.SaveChanges();
                }

                void Body(int i)
                {
                    using (var session = store.OpenSession())
                    {
                        var lazy1 = session.Advanced.Lazily.Load<Document>(doc2.Id);
                        var lazy2 = session.Advanced.Lazily.Load<Document>(doc3.Id);

                        List<string> targetIds = new List<string>
                        {
                            "id2"
                        };

                        var projection =
                            from d in session.Query<Document>().Where(x => x.Id == doc1.Id)
                            let doc = d.SubDocuments.FirstOrDefault(x => x.Id == "testing")
                            select new
                            {
                                d.Id,
                                d.Deleted,
                                SubTestId = doc != null ? doc.Id : null,
                                Values = d.SubDocuments
                                    .Where(x => targetIds.Count == 0 || targetIds.Contains(x.TargetId))
                                    .Select(x => new
                                    {
                                        x.TargetId,
                                        x.TargetValue,
                                        SubDocuments = x.SubDocuments
                                            .Where(s => s != null)
                                            .Select(s => new
                                            {
                                                s.TargetId,
                                                s.TargetValue
                                            })
                                            .ToList()
                                    })
                                    .ToList()
                            };

                        var list = projection.ToList();

                        Assert.NotEmpty(list);
                    }
                }

                Parallel.For(0, 1000, RavenTestHelper.DefaultParallelOptions, Body);
            }
        }
    }

}
