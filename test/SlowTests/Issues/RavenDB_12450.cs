using System.Linq;
using FastTests;
using Raven.Client.Documents.Indexes;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12450 : RavenTestBase
    {
        [Fact]
        public void ProjectingIdFromMapReduceIndex()
        {
            using (var store = GetDocumentStore())
            {
                new DocumentIndex().Execute(store);

                using (var s = store.OpenSession())
                {
                    s.Store(new Document
                    {
                        Name = "name"
                    });

                    s.Advanced.WaitForIndexesAfterSaveChanges(indexes: new[] { nameof(DocumentIndex) });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var query = from doc in s.Query<Document, DocumentIndex>()
                                select new
                                {
                                    doc.Id,
                                    doc.Name
                                };

                    var item = query.Single();
                    Assert.NotNull(item);
                    Assert.NotNull(item.Id);
                    Assert.Equal("name", item.Name);
                }
            }
        }

        private class Document
        {
            public string Id { get; set; }
            public string Name { get; set; }
        }

        private class DocumentIndex : AbstractMultiMapIndexCreationTask<DocumentIndex.Result>
        {
            public class Result
            {
                public string Id { get; set; }
                public string Name { get; set; }
            }

            public DocumentIndex()
            {
                AddMap<Document>(docs => from doc in docs
                                         select new
                                         {
                                             doc.Id,
                                             doc.Name,
                                         });

                Reduce = results => from result in results
                                    group result by result.Id
                    into g
                                    select new
                                    {
                                        Id = g.Key,
                                        Name = g.Select(x => x.Name).First(x => !string.IsNullOrEmpty(x)),
                                    };

                // does not help
                Store(x => x.Id, FieldStorage.Yes);
            }
        }
    }
}
