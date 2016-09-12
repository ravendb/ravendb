using FastTests;
using Raven.Abstractions.Data;
using Raven.Client.Data;
using Raven.Client.Document;
using Raven.Client.Indexing;
using Xunit;

namespace SlowTests.MailingList
{
    public class TomCabanski : RavenTestBase
    {
        [Fact]
        public void CanEscapeGetFacets()
        {
            using (var store = GetDocumentStore())
            {
                store.DatabaseCommands.PutIndex("test", new IndexDefinition
                {
                    Maps = { "from doc in docs.Users select new { doc.Age, doc.IsActive, doc.BookVendor }" }
                });

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup
                    {
                        Id = "facets/test",
                        Facets =
                            {
                                new Facet
                                {
                                    Mode = FacetMode.Default,
                                    Name = "Age"
                                }
                            }
                    });
                    s.SaveChanges();
                }

                store.DatabaseCommands.GetFacets(new FacetQuery
                {
                    IndexName = "test",
                    Query = "(IsActive:true)  AND (BookVendor:\"stroheim & romann\")",
                    FacetSetupDoc = "facets/test"
                });
            }
        }
    }
}
