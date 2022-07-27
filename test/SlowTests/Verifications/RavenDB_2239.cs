using System;
using System.Diagnostics;
using System.Linq;
using FastTests;
using Tests.Infrastructure;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Verifications
{
    public class RavenDb_2239 : RavenTestBase
    {
        public RavenDb_2239(ITestOutputHelper output) : base(output)
        {
        }

        private class Document
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
            public int Num { get; set; }

        }

        private class DocumentDivided
        {
            public double a { get; set; }
        }

        private class DocumentName
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Num { get; set; }

        }

        private class Document_Index : AbstractIndexCreationTask<Document>
        {
            public Document_Index()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  doc.Name,
                              };
            }
        }
        private class TestDocument_Index : AbstractIndexCreationTask<Document>
        {
            public TestDocument_Index()
            {
                Map = docs => from doc in docs
                              select new
                              {
                                  doc.Id,
                                  doc.Name,
                                  doc.Num,
                              };
            }
        }
        private void TestSetupData(IDocumentStore store)
        {
            new Document_Index().Execute(store);

            using (var session = store.OpenSession())
            {

                for (int docId = 10, i = 0; docId >= 0; docId--, i++)
                {
                    session.Store(new Document
                    {
                        Id = "documents/" + i,
                        Name = "Doc" + i,
                        Description = "Test document description for " + docId,
                        Num = docId
                    });
                }
                session.SaveChanges();
            }


            Indexes.WaitForIndexing(store);
        }


        private void SetupData(IDocumentStore store)
        {
            new Document_Index().Execute(store);


            var docsToCreate = Enumerable.Range(1, 50000);
            var skip = 0;

            while (true)
            {
                var batch = docsToCreate.Skip(skip).Take(256).ToList();

                if (!batch.Any())
                    break;
                skip += batch.Count;

                using (var session = store.OpenSession())
                {
                    foreach (var docId in batch)
                    {
                        session.Store(new Document
                        {
                            Id = "documents/" + docId,
                            Name = "Doc" + docId,
                            Description = "Test document description for " + docId,
                        });
                    }
                    session.SaveChanges();
                }
            }

            Indexes.WaitForIndexing(store);
        }

        [Theory]
        [RavenData(JavascriptEngineMode = RavenJavascriptEngineMode.Jint)]
        public void SmallLogTransformerTest(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                var sw = new Stopwatch();
                sw.Restart();
                TestSetupData(store);
                using (var session = store.OpenSession())
                {
                    var cntr = 0;
                    sw.Restart();

                    var script = @"
declare function get(d){
    if(d.Num == 0) {
        return {}.DoesNotExistsAndWillThrow();
    }
    return { a: 100 / d.Num };
}
from index 'Document/Index' as d
select get(d)
";
                    var ravenQueryable = session.Advanced
                        .RawQuery<DocumentDivided>(script);
                    
                    using (var enumerator = session.Advanced.Stream(ravenQueryable))
                    {
                        bool more = enumerator.MoveNext();
                        sw.Stop();
                        Trace.WriteLine("Time to first result with transformer: " + sw.Elapsed);
                        Assert.True(more);

                        while (more)
                        {
                            sw.Restart();
                            Trace.WriteLine("Time to first result with transformer: " + sw.Elapsed);

                            if (cntr != 10)
                            {
                                Assert.Equal(100.0 / (10 - cntr), enumerator.Current.Document.a);
                            }

                            cntr++;
                            
                            if (cntr == 10)
                            {
                                Assert.Throws<InvalidOperationException>(() =>
                                {
                                    more = enumerator.MoveNext();
                                });
                            }
                            else
                            {
                                more = enumerator.MoveNext();
                            }

                        }
                    }
                    Assert.True(cntr == 11, $"{cntr} == 11");
                }

            }
        }

        [Fact]
        public void FullLogTransformerDelay()
        {
            using (var store = GetDocumentStore())
            {
                var withTransformer = new Stopwatch();
                var withoutTransformer = new Stopwatch();

                var sw = new Stopwatch();
                sw.Restart();
                SetupData(store);
                Trace.WriteLine(" fill db finished " + sw.Elapsed);
                for (int i = 0; i < 3; i++)
                {
                    using (var session = store.OpenSession())
                    {
                        sw.Restart();
                        var query = session.Advanced.RawQuery<Document>("from index 'Document/Index' as d select d.Id, d.Name");
                        using (var enumerator = session.Advanced.Stream(query))
                        {
                            enumerator.MoveNext();
                            sw.Stop();
                            Trace.WriteLine("Time to first result with transformer: " + sw.Elapsed);
                            while (enumerator.MoveNext())
                            {

                            }
                        }
                        if (i == 2)
                        {
                            withTransformer = sw;
                        }
                    }
                    using (var session = store.OpenSession())
                    {
                        sw.Restart();
                        using (var enumerator = session.Advanced.Stream(session.Query<Document, Document_Index>()))
                        {
                            enumerator.MoveNext();
                            sw.Stop();
                            while (enumerator.MoveNext())
                            {

                            }
                        }
                    }
                    if (i == 2)
                    {
                        withoutTransformer = sw;
                    }
                }
                Assert.True(withTransformer.Elapsed.TotalMilliseconds <= withoutTransformer.Elapsed.TotalMilliseconds * 1.3);
            }
        }
    }
}
