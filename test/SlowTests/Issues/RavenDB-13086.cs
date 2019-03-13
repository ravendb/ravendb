using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Raven.Client.Documents.Queries;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_13086 : RavenTestBase
    {
        [Fact]
        public void ResultCacheShouldConsiderDocumentsLoadedInProjection()
        {
            using (var store = GetDocumentStore())
            {
                Document mainDocument;
                Document2 referencedDocument;
                using (var session = store.OpenSession())
                {
                    referencedDocument = new Document2
                    {
                        DataToUpdate = "original"
                    };
                    session.Store(referencedDocument);

                    mainDocument = new Document
                    {
                        References = new[]
                        {
                            new DocumentReference
                            {
                                Document2Id = referencedDocument.Id
                            },
                        }
                    };
                    session.Store(mainDocument);
                    session.SaveChanges();
                }

                string[] ProjectValuesLinq()
                {
                    using (var session = store.OpenSession())
                    {
                        var single = (from doc in session.Query<Document>().Where(x => x.Id == mainDocument.Id)
                                let referenced = RavenQuery.Load<Document2>(doc.References.Select(x => x.Document2Id))
                                select new Result
                                {
                                    Data = referenced
                                        .Select(x => x.DataToUpdate)
                                        .ToArray()
                                })
                            .Single();

                        return single.Data;
                    }
                }

                Assert.Contains(ProjectValuesLinq(), x => x == "original");

                using (var session = store.OpenSession())
                {
                    var dbDoc = session.Load<Document2>(referencedDocument.Id);
                    dbDoc.DataToUpdate = "modified";
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var dbDoc = session.Load<Document2>(referencedDocument.Id);
                    Assert.Equal("modified", dbDoc.DataToUpdate);
                }

                Assert.Contains(ProjectValuesLinq(), x => x == "modified");
            }
        }

        private class Document
        {
            public string Id;
            public DocumentReference[] References;
        }

        private class DocumentReference
        {
            public string Document2Id;
        }

        private class Document2
        {
            public string Id;
            public string DataToUpdate;
        }
    }
}
