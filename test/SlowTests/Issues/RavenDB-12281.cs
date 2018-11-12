using System.Linq;
using FastTests;
using Raven.Client.Documents.Linq;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12281 : RavenTestBase
    {
        private class Document
        {
            public string Id { get; set; }
            public DocumentStatus Status { get; set; }
        }

        private enum DocumentStatus
        {
            Unknown,
            Success
        }

        [Fact]
        public void EnumComparisonWithLet()
        {
            using (var store = GetDocumentStore())
            {
                string id = "docs/1";

                using (var s = store.OpenSession())
                {
                    s.Store(new Document
                    {
                        Id = id,
                        Status = DocumentStatus.Success
                    });

                    s.SaveChanges();
                }

                using (var s = store.OpenSession())
                {
                    var query = from doc in s.Query<Document>()
                        select new
                        {
                            False = doc.Status != DocumentStatus.Success,
                            SecondFalse = !(doc.Status == DocumentStatus.Success),
                            True = doc.Status == DocumentStatus.Success,
                            SecondTrue = !(doc.Status != DocumentStatus.Success),
                        };

                    Assert.Equal("from Documents as doc select { " +
                                 "False : doc.Status!==\"Success\", " +
                                 "SecondFalse : !(doc.Status===\"Success\"), " +
                                 "True : doc.Status===\"Success\", " +
                                 "SecondTrue : !(doc.Status!==\"Success\") }" , query.ToString());

                    var item = query.Single();
                    Assert.NotNull(item);
                    Assert.False(item.False);
                    Assert.False(item.SecondFalse);
                    Assert.True(item.True);
                    Assert.True(item.SecondTrue);
                }
            }
        }
    }
}
