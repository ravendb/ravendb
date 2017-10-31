using System.Linq;
using FastTests;
using Raven.Client.Documents;
using Raven.Client.Documents.Indexes;
using Raven.Client.Documents.Operations.Indexes;
using Raven.Client.Documents.Queries.Facets;
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
                store.Admin.Send(new PutIndexesOperation(new[] {new IndexDefinition
                {
                    Name ="test",
                    Maps = { "from doc in docs.Users select new { doc.Age, doc.IsActive, doc.BookVendor }" }
                }}));

                using (var s = store.OpenSession())
                {
                    s.Store(new FacetSetup
                    {
                        Id = "facets/test",
                        Facets =
                            {
                                new Facet
                                {
                                    Name = "Age"
                                }
                            }
                    });
                    s.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    session.Query<User>("test")
                        .Where(x => x.IsActive && x.BookVendor == "stroheim & romann")
                        .AggregateUsing("facets/test")
                        .ToDictionary();
                }
            }
        }

        private class User
        {
#pragma warning disable 649
            public bool IsActive;

            public string BookVendor;
#pragma warning restore 649
        }
    }
}
