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

        
            var docsToCreate = Enumerable.Range(1, 50000); 
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
            using (var store = NewDocumentStore()) 
            {
                var sw = new Stopwatch();
                sw.Restart();
                TestSetupData(store);
                using (var session = store.OpenSession())
                {
                    var cntr = 0;
                    sw.Restart();
                    Assert.Throws<AggregateException>(() =>
                    {
                        using (
                            var enumerator =
                                session.Advanced.Stream(
                                    session.Query<Document, Document_Index>()
                                        .TransformWith<TestDocumentNameTransformer, DocumentName>()))
                        {
                            enumerator.MoveNext();
                            sw.Stop();
                            Trace.WriteLine("Time to first result with transformer: " + sw.Elapsed);
                            cntr++;
                            while (enumerator.MoveNext())
                            {
                                sw.Restart();
                                Trace.WriteLine("Time to first result with transformer: " + sw.Elapsed);
                                cntr++;
                            }
                        }
                    });
                    Assert.True(cntr== 10);

               }

            }
        }
        

        
        [Fact]
        public void FullLogTransformerDelay()
        {
  
            using (var store = NewDocumentStore()) 
            {
                var withTransformer = new Stopwatch();
                var withoutTransformer =new  Stopwatch(); 

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
                Assert.True(withTransformer.Elapsed.TotalMilliseconds <= withoutTransformer.Elapsed.TotalMilliseconds*1.3);
            }
        }
       
      
    }
}
