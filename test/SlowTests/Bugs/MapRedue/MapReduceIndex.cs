using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Queries;
using Raven.Client.Documents.Session;
using Xunit;

namespace SlowTests.Bugs.MapRedue
{
    public class MapReduceIndex : RavenTestBase
    {
        private readonly String[] m_documentIds;

        public MapReduceIndex()
        {
            m_documentIds = new String[5]
            {
                "One",
                "Two",
                "Three",
                "Four ",
                "Five"
            };
        }

        [Fact]
        public void MapReduceIndexTest()
        {
            using (var store = GetDocumentStore())
            {
                new VersionedDocuments().Execute(store);

                RemoveAllDocuments(store);

                // index should return no document
                Assert.Equal(0, NumberOfDocumentsInDbByIndex(store));

                // insert documents for all ids
                using (var session = store.OpenSession())
                {
                    for (int index = 0; index < m_documentIds.Length; index++)                    
                        InserDocumentIntoDb(session, m_documentIds[index], Convert.ToByte(index + 1));
                                                          
                    session.SaveChanges();
                }

                // index should return number of document equal to number of inserted
                Assert.Equal(m_documentIds.Length,
                             NumberOfDocumentsInDbByIndex(store));

                RemoveAllDocuments(store);

                // index should return no document
                var stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(0, stats.CountOfDocuments);

                // insert documents one document with the same id as one of documents inserted before
                using (IDocumentSession session = store.OpenSession())
                {
                    InserDocumentIntoDb(session, m_documentIds[0], 1);

                    session.SaveChanges();
                }

                // index should return one document
                stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfDocuments);
            }
        }

        private class Document
        {
            /// <summary>
            /// Document Id
            /// </summary>
            public String Id { get; set; }

            /// <summary>
            /// Date when document was "removed" (disabled)
            /// </summary>
            public DateTime? DateRemoved { get; set; }

            /// <summary>
            /// Array containing versions of document
            /// </summary>
            public VersionedDocument[] Versions { get; set; }
        }

        private class DocumentView
        {
            /// <summary>
            /// Document id
            /// </summary>
            public String Id { get; set; }

            /// <summary>
            /// Version number of document
            /// </summary>
            public uint Version { get; set; }

            /// <summary>
            /// Instace of versioned document with the same version number
            /// </summary>
            public VersionedDocument Document { get; set; }
        }

        private class VersionedDocument
        {
            /// <summary>
            /// Document id
            /// </summary>
            public string Id { get; set; }

            /// <summary>
            /// Version number of document
            /// </summary>
            public uint Version { get; set; }

            /// <summary>
            /// Some data included in document
            /// </summary>
            public string Data { get; set; }
        }

        private class VersionedDocuments : AbstractIndexCreationTask<Document, DocumentView>
        {
            public VersionedDocuments()
            {
                Map = aDocuments =>
                      from document in aDocuments
                      from version in document.Versions
                      select new
                      {
                          document.Id,
                          version.Version,
                          Document = version
                      };
                Reduce = aResults =>
                         from result in aResults
                         group result by result.Id
                         into g
                         select new
                         {
                             Id = g.Key,
                             g.Where(aView => aView.Version ==
                                          g.Max(aView2 => aView2.Version)).FirstOrDefault().Version,
                             g.Where(aView => aView.Version ==
                                          g.Max(aView2 => aView2.Version)).FirstOrDefault().Document
                         };

                Store(x => x.Version, FieldStorage.Yes);
                Store(x => x.Document, FieldStorage.Yes);
            }
        }

        private void RemoveAllDocuments(DocumentStore store)
        {
            store.Operations.Send(new DeleteByQueryOperation(new IndexQuery() { Query = "FROM Documents" })).WaitForCompletion(TimeSpan.FromSeconds(15));
        }

        private void InserDocumentIntoDb(IDocumentSession aSession, String aDocumentId, Byte aVersionCount)
        {
            var versions = new List<VersionedDocument>(aVersionCount);
            for (int index = 0; index < aVersionCount; index++)
            {
                versions.Add(new VersionedDocument
                {
                    Id = aDocumentId,
                    Version = Convert.ToUInt32(index+ 1),
                    Data = String.Format("Data for version {0}", index + 1)
                });
            }

            var document = new Document
            {
                Id = aDocumentId,
                Versions = versions.ToArray()
            };

            aSession.Store(document);
        }

        private int NumberOfDocumentsInDbByIndex(IDocumentStore aStore)
        {
            using (IDocumentSession session = aStore.OpenSession())
            {
                IRavenQueryable<DocumentView> query =
                    session.Query<Document, VersionedDocuments>()
                        .Customize(aCustomization => aCustomization.WaitForNonStaleResults(TimeSpan.FromMinutes(10)))
                        .ProjectInto<DocumentView>();

                foreach (DocumentView document in query)
                {
                    Debug.WriteLine(String.Format("Document {0} v.{1}", document.Id, document.Version));
                }

                return query.Count();
            }
        }
    }
}
