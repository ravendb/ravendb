using System.Linq;
using FastTests;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12056 : RavenTestBase
    {
        [Fact]
        public void CountWorksWithPredicate()
        {
            const string documentId = "document-id";

            using (var store = GetDocumentStore())
            {
                using (var s = store.OpenSession())
                {
                    s.Store(new Document
                    {
                        Id = documentId,
                        Items = new[]
                        {
                            new DocumentItem
                            {
                                Failed = null,
                                Result = null
                            },
                            new DocumentItem
                            {
                                Failed = true,
                                Result = 123
                            },
                            new DocumentItem
                            {
                                Failed = false,
                                Result = -123
                            }
                        }
                    });
                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var queryable = s.Query<Document>()
                        .Where(x => x.Id == documentId)
                        .Select(x => new
                        {
                            ItemsLength = x.Items.Length,
                            ItemsCount = x.Items.Count(),
                            FailedCount = x.Items.Count(i => i.Failed == true),
                            SuccessCount = x.Items.Count(i => i.Failed == false),
                            UnknownFailedCount = x.Items.Count(i => i.Failed == null),
                            NegativeResultCount = x.Items.Count(i => i.Result < 0),
                            PositiveResultCount = x.Items.Count(i => i.Result > 0),
                            UnknownResultCount = x.Items.Count(i => i.Result  == null),
                            ExactResultCount = x.Items.Count(i => i.Result  == 123)
                        });
                    
                    var stats = queryable
                        .SingleOrDefault();

                    Assert.NotNull(stats);
                    Assert.Equal(3, stats.ItemsLength);
                    Assert.Equal(3, stats.ItemsCount);
                    Assert.Equal(1, stats.FailedCount);
                    Assert.Equal(1, stats.SuccessCount);
                    Assert.Equal(1, stats.UnknownFailedCount);
                    Assert.Equal(1, stats.NegativeResultCount);
                    Assert.Equal(1, stats.PositiveResultCount);
                    Assert.Equal(1, stats.UnknownResultCount);
                    Assert.Equal(1, stats.ExactResultCount);
                }
            }
        }

        private class Document
        {
            public string Id { get; set; }
            public DocumentItem[] Items { get; set; }
        }

        private class DocumentItem
        {
            public bool? Failed { get; set; }
            public int? Result { get; set; }
        }
    }
}
