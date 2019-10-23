using Xunit;
using Xunit.Abstractions;

namespace FastTests.Client
{
    public class Issue_6069 : RavenTestBase
    {
        public Issue_6069(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CRUD_Operations()
        {
            using (var store = GetDocumentStore())
            {
                using (var newSession = store.OpenSession())
                {
                    var doc = new Document()
                    {
                        FreshDays = null,
                        GrossWeight = null,
                        NetWeight = null
                    };
                    newSession.Store(doc, "docs/1");
                    newSession.SaveChanges();
                 }

                using (var session = store.OpenSession())
                {
                    var doc = session.Load<Document>("docs/1");
                    Assert.NotNull(doc);
                    Assert.Null(doc.FreshDays);
                    Assert.Null(doc.GrossWeight);
                    Assert.Null(doc.NetWeight);
                }
            }
        }

        private class Document
        {
            public int? FreshDays { get; set; }
            public int? GrossWeight { get; set; }
            public int? NetWeight { get; set; }
        }
    }
}
