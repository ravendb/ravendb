using Raven.Client;
using Raven.Client.Indexes;
using Raven.Tests.Helpers;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Queries
{
    public class RavenDb_2239 : RavenTestBase
    {
        public class Document
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
            public int Num { get; set; }

        }

        public class DocumentName
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public int Num { get; set; }

        }

        public class Document_Index : AbstractIndexCreationTask<Document>
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
        public class TestDocument_Index : AbstractIndexCreationTask<Document>
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
        public class TestDocumentNameTransformer : AbstractTransformerCreationTask<Document>
        {
            public TestDocumentNameTransformer()
            {
                TransformResults = docs => from doc in docs
                                           select new { a = 100 / doc.Num };
            }
        }

        public void TestSetupData(IDocumentStore store)
        {
            new Document_Index().Execute(store);
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
        public class DocumentNameTransformer : AbstractTransformerCreationTask<Document>
        {
            public DocumentNameTransformer()
            {
                TransformResults = docs => from doc in docs
                                           select new { doc.Id, doc.Name };
            }
        }


        private void SetupData(IDocumentStore store)
        {
            new Document_Index().Execute(store);
            new DocumentNameTransformer().Execute(store);

            //   var docsToCreate = Enumerable.Range(1, 100000);
            var docsToCreate = Enumerable.Range(1, 200000); //15000 ok, 30 crash time for transformer 15K 00:02:19.5786329 without 00:01:25.3800946
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
            using (var store = NewDocumentStore()) //requestedStorage: "esent"))
            {
                var sw = new Stopwatch();
                sw.Restart();
                TestSetupData(store);
                Trace.WriteLine(" fill db finished " + sw.Elapsed);
                using (var session = store.OpenSession())
                {
                    sw.Restart();
                    Trace.WriteLine("Before Stream Query");
                    using (var enumerator = session.Advanced.Stream(session.Query<Document, Document_Index>().TransformWith<TestDocumentNameTransformer, DocumentName>()))
                    {
                        Trace.WriteLine("started streaming");
                        enumerator.MoveNext();
                        Trace.WriteLine("move next first time");
                        sw.Stop();
                        Trace.WriteLine("Time to first result with transformer: " + sw.Elapsed);
                        while (enumerator.MoveNext())
                        {
                            sw.Restart();
                            Trace.WriteLine("Time to first result with transformer: " + sw.Elapsed);
                        }
                    }
                }

            }
        }
        [Fact]
        public void Profiling()
        {
            // using (var store = NewRemoteDocumentStore(fiddler: true)) //requestedStorage: "esent"))
            Trace.WriteLine("start LogTransformerDelay fill db " + DateTime.UtcNow);
            using (var store = NewDocumentStore()) //requestedStorage: "esent"))
            {
                SetupData(store);
            }
            //using (var store = new EmbeddedDocumentStore { DataDirectory = "\\Data" }) //requestedStorage: "esent"))
            //{
            //    store.Initialize();
            //    SetupData(store);
            //}
        }


        [Fact]
        public void LogTransformerDelay()
        {
           // using (var store = NewRemoteDocumentStore(fiddler: true)) //requestedStorage: "esent"))
            Trace.WriteLine("start LogTransformerDelay fill db "+ DateTime.UtcNow);
            using (var store = NewDocumentStore()) //requestedStorage: "esent"))
            {
                SetupData(store);
                Trace.WriteLine(" fill db finished " + DateTime.UtcNow);

                var sw = new Stopwatch();
                using (var session = store.OpenSession())
                {

                    sw.Restart();
                    using (var enumerator = session.Advanced.Stream(session.Query<Document, Document_Index>().TransformWith<DocumentNameTransformer, DocumentName>()))
                    {
                        enumerator.MoveNext();
                        sw.Stop();
                        System.Diagnostics.Trace.WriteLine("Time to first result with transformer: " + sw.Elapsed);
                        while (enumerator.MoveNext())
                        {

                        }
                    }
                }

                Trace.WriteLine("finished " + DateTime.UtcNow);

            }
        }
        [Fact]
        public void FullLogTransformerDelay()
        {
            using (var store = NewDocumentStore()) //requestedStorage: "esent"))
            {
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
                }
            }
        }
        [Fact]
        public void LogWithoutTransformer()
        {
            using (var store = NewRemoteDocumentStore(fiddler: true)) //requestedStorage: "esent"))
            {
                SetupData(store);

                var sw = new Stopwatch();
              
                using (var session = store.OpenSession())
                {
                    sw.Restart();
                    using (var enumerator = session.Advanced.Stream(session.Query<Document, Document_Index>()))
                    {
                        enumerator.MoveNext();
                        sw.Stop();
                        System.Diagnostics.Trace.WriteLine("Time to first result: " + sw.Elapsed);
                        while (enumerator.MoveNext())
                        {

                        }
                    }
                }


            }
        }
    }
}
