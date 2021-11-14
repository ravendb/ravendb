using System.Linq;
using FastTests;
using FastTests.Server.JavaScript;
using Raven.Client.Documents.Linq;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_12281 : RavenTestBase
    {
        public RavenDB_12281(ITestOutputHelper output) : base(output)
        {
        }

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

        [Theory]
        [JavaScriptEngineClassData]
        public void EnumComparisonWithLet(string jsEngineType)
        {
            using (var store = GetDocumentStore(Options.ForJavaScriptEngine(jsEngineType)))
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

                    Assert.Equal("from 'Documents' as doc select { " +
                                 "False : doc?.Status!=='Success', " +
                                 "SecondFalse : !(doc?.Status==='Success'), " +
                                 "True : doc?.Status==='Success', " +
                                 "SecondTrue : !(doc?.Status!=='Success') }" , query.ToString());

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
