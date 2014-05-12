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


        private void SetupData(IDocumentStore store)
        {
            new Document_Index().Execute(store);
            new DocumentNameTransformer().Execute(store);

         //   var docsToCreate = Enumerable.Range(1, 100000);
            var docsToCreate = Enumerable.Range(1, 100000);
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
        public void LogTransformerDelay()
        {
            using (var store = NewDocumentStore()) //requestedStorage: "esent"))
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
            }
        }
    }
}

