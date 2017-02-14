using Xunit;

namespace FastTests.Client
{
    public class Issue_6069 : RavenTestBase
    {
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

        public class Document
        {
            public int? FreshDays { get; set; }
            public int? GrossWeight { get; set; }
            public int? NetWeight { get; set; }
        }
    }
}
