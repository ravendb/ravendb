using System;
using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12066 : RavenTestBase
    {
        [Fact]
        public void FilteredMinAndMaxProjectionAgainstEmptyCollection()
        {
            const string documentId = "document-id";

            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Document
                    {
                        Id = documentId,
                        Items = Array.Empty<DocumentItem>()
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var queryable = s.Query<Document>()
                        .Where(x => x.Id == documentId)
                        .Select(x => new Result
                        {
                            FailedMin = x.Items.Where(i => i.Failed).Min(i => i.Result),
                            FailedMax = x.Items.Where(i => i.Failed).Max(i => i.Result)
                        });

                    var stats = queryable
                        .SingleOrDefault();

                    Assert.NotNull(stats);
                    Assert.Equal(null, stats.FailedMax);
                    Assert.Equal(null, stats.FailedMin);
                }
            }
        }

        private class Result
        {
            public int? FailedMin { get; set; }
            public int? FailedMax { get; set; }
        }

        private class Document
        {
            public string Id { get; set; }
            public DocumentItem[] Items { get; set; }
        }

        private class DocumentItem
        {
            public bool Failed { get; set; }
            public int? Result { get; set; }
        }
    }
}
