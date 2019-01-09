using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Queries;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12650 : RavenTestBase
    {
        [Fact]
        public void LoadingDocumentInProjectionUsingStoredIndexIdInMapReduceIndex()
        {
            using (var store = GetDocumentStore())
            {
                new DocumentIndex().Execute(store);

                using (var session = store.OpenSession())
                {
                    session.Store(new Document
                    {
                        Id = "doc-id",
                        Name = "doc name",
                        ExtraProperty = "extra property"
                    });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var ravenQueryable = session.Query<DocumentIndex.Result, DocumentIndex>()
                        .Customize(x => x.WaitForNonStaleResults());

                    var query = from x in ravenQueryable
                                let doc = RavenQuery.Load<Document>(x.DocumentId)
                                select new
                                {
                                    doc.Id,
                                    doc.Name,
                                    doc.ExtraProperty
                                };

                    Assert.Equal("from index 'DocumentIndex' as x load x.DocumentId as doc select { Id : id(doc), Name : doc.Name, ExtraProperty : doc.ExtraProperty }",
                        query.ToString());

                    var result = query.SingleOrDefault();

                    Assert.NotNull(result);
                    Assert.Equal("doc-id", result.Id);
                    Assert.Equal("doc name", result.Name);
                    Assert.Equal("extra property", result.ExtraProperty);
                }
            }
        }

        private class Document
        {
            public string Id { get; set; }
            public string Name { get; set; }
            public string ExtraProperty { get; set; }
        }

        private class DocumentIndex : AbstractMultiMapIndexCreationTask<DocumentIndex.Result>
        {
            public class Result
            {
                public string DocumentId { get; set; }
                public string Name { get; set; }
            }

            public DocumentIndex()
            {
                AddMap<Document>(docs => from doc in docs
                                         select new
                                         {
                                             DocumentId = doc.Id,
                                             doc.Name,
                                         });

                Reduce = results => from result in results
                                    group result by result.DocumentId
                    into g
                                    select new
                                    {
                                        DocumentId = g.Key,
                                        Name = g.Where(x => x.Name != null).Select(x => x.Name).First(),
                                    };

                Store(x => x.DocumentId, FieldStorage.Yes);
            }
        }
    }
}
