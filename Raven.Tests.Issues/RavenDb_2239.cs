using Raven.Client;
using Raven.Client.Document;
using Raven.Client.Indexes;
using Raven.Database.Client;
using Raven.Tests.Helpers;
using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Xunit;

namespace Raven.Tests.Issues
{
    public class RavenDb_2239 : RavenTestBase
    {
        public class Document
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string Description { get; set; }
        }

        public class DocumentName
        {
            public string Id { get; set; }
            public string Name { get; set; }
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

        public class DocumentNameTransformer : AbstractTransformerCreationTask<Document>
        {
            public DocumentNameTransformer()
            {
                TransformResults = docs => from doc in docs
                                           select new { doc.Id, doc.Name };
            }
        }


        public void SetupData(IDocumentStore store)
        {
            new Document_Index().Execute(store);
            new DocumentNameTransformer().Execute(store);

         //   var docsToCreate = Enumerable.Range(1, 100000);
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
        public void Profiling()
        {
            Trace.WriteLine("start LogTransformerDelay fill db " + DateTime.UtcNow);
            //using (var store = NewRemoteDocumentStore(fiddler: true)) //requestedStorage: "esent"))
             using (var store = new EmbeddedDocumentStore { DataDirectory = "\\Data" }) //requestedStorage: "esent"))
             {
                store.Initialize();
                SetupData(store);
             }
        }

        [Fact]
        public void LogTransformerDelay()
        {
            Trace.WriteLine("start LogTransformerDelay fill db " + DateTime.UtcNow);
            using (var store = NewRemoteDocumentStore(fiddler: true)) //requestedStorage: "esent"))
          //  using (var store = NewDocumentStore()) //requestedStorage: "esent"))
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
                        Trace.WriteLine("Time to first result with transformer: " + sw.Elapsed);
                        while (enumerator.MoveNext())
                        {

                        }
                    }
                    Trace.WriteLine("transformer  test finished " + DateTime.UtcNow);
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
                Trace.WriteLine(" test finished " + DateTime.UtcNow);

                
            }
        }
        [Fact]
        public void LogWithoutTransformer()
        {
            using (var store = NewRemoteDocumentStore(fiddler: true)) //requestedStorage: "esent"))
            {
                SetupData(store);

                var sw = new Stopwatch();
                //using (var session = store.OpenSession())
                //{

                //    sw.Restart();
                //    using (var enumerator = session.Advanced.Stream(session.Query<Document, Document_Index>().TransformWith<DocumentNameTransformer, DocumentName>()))
                //    {
                //        enumerator.MoveNext();
                //        sw.Stop();
                //        System.Diagnostics.Trace.WriteLine("Time to first result with transformer: " + sw.Elapsed);
                //        while (enumerator.MoveNext())
                //        {

                //        }
                //    }
                //}
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

