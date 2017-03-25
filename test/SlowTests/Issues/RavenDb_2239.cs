using System;
using System.Diagnostics;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Transformers;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDb_2239 : RavenTestBase
    {
        private class Document
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
            public int Num { get; set; }

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

        private class DocumentNameTransformer : AbstractTransformerCreationTask<Document>
        {
            public DocumentNameTransformer()
            {
                TransformResults = docs => from doc in docs
                                           select new { doc.Id, doc.Name };
            }
        }

        private class TestDocumentNameTransformer : AbstractTransformerCreationTask<Document>
        {
            public TestDocumentNameTransformer()
            {
                TransformResults = docs => from doc in docs
                                           select new { a = 100 / doc.Num };
            }
        }

        private void TestSetupData(IDocumentStore store)
        {
            new TestDocument_Index().Execute(store);
            new TestDocumentNameTransformer().Execute(store);


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


            WaitForIndexing(store);
        }

        private void SetupData(IDocumentStore store)
        {
            new Document_Index().Execute(store);
            new DocumentNameTransformer().Execute(store);

            var docsToCreate = Enumerable.Range(1, 20000); //15000 ok, 30 crash time for transformer 15K 00:02:19.5786329 without 00:01:25.3800946
            var skip = 0;

            while (true)
            {
                var batch = docsToCreate.Skip(skip).Take(256).ToList();

                if (!batch.Any()) break;
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

            WaitForIndexing(store);
        }

        [Fact]
        public void SmallLogTransformerTest()
        {
            using (var store = GetDocumentStore())
            {
                var sw = new Stopwatch();
                sw.Restart();
                TestSetupData(store);

                int cntr = 0;
                using (var session = store.OpenSession())
                {
                    Assert.Throws<InvalidOperationException>(() =>
                    {
                        using (
                            var enumerator =
                                session.Advanced.Stream(
                                    session.Query<Document, TestDocument_Index>()
                                        .OrderByDescending(x => x.Num)
                                        .TransformWith<TestDocumentNameTransformer, DocumentName>()))
                        {
                            enumerator.MoveNext();
                            sw.Stop();
                            sw.Restart();
                            cntr++;
                            while (enumerator.MoveNext())
                            {
                                cntr++;
                            }

                        }
                    });
                    Assert.True(cntr == 10);
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
                        using (var enumerator = session.Advanced.Stream(session.Query<Document, Document_Index>().TransformWith<DocumentNameTransformer, DocumentName>()))
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
                            Trace.WriteLine("Time to first result: " + sw.Elapsed);
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

