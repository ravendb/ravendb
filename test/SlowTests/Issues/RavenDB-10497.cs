using Tests.Infrastructure;
using System;
using System.Linq;
using System.Collections.Generic;
using FastTests;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10497 : RavenTestBase
    {
        public RavenDB_10497(ITestOutputHelper output) : base(output)
        {
        }

        public class Document
        {
            public string Id { get; set; }
            public DateTimeOffset? Date { get; set; }
            public List<Document> SubDocuments { get; set; }
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void CanQueryMinMaxDatesCoalescing(Options options)
        {
            DateTimeOffset date = DateTimeOffset.UtcNow.Date;
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    var doc = new Document
                    {
                        Date = date,
                        SubDocuments = new List<Document>
                        {
                            new Document(),
                            new Document()
                        }
                    };

                    session.Store(doc);
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var projection =
                        from d in session.Query<Document>()
                            .Customize(x => x.WaitForNonStaleResults())
                        let min = d.SubDocuments.Min(x => x.Date)
                        let max = d.SubDocuments.Max(x => x.Date)
                        select new
                        {
                            DateMin = min ?? d.Date,
                            DateMax = max ?? d.Date
                        };

                    WaitForUserToContinueTheTest(store);

                    var projectionResult = projection.ToList();

                    Assert.Equal(1, projectionResult.Count);
                    Assert.Equal(date, projectionResult[0].DateMin);
                    Assert.Equal(date, projectionResult[0].DateMax);
                }
            }
        }
    }
}
