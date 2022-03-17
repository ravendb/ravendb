using System.Linq;
using FastTests;
using Orders;
using Raven.Client.Documents;
using Raven.Client.Documents.Operations.Indexes;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13203 : RavenTestBase
    {
        public RavenDB_13203(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void TermsShouldNotBeReturnedIfThereAreNoMatchingDocumentsForThem()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "Company1"
                    });

                    session.Store(new Company
                    {
                        Name = "Company1"
                    });

                    session.Store(new Company
                    {
                        Name = "Company2"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var companies = session.Query<Company>()
                        .Statistics(out var stats)
                        .Where(x => x.Name != "Hello")
                        .OrderBy(x => x.Name)
                        .ToList();

                    Assert.Equal(3, companies.Count);

                    var terms = store.Maintenance.Send(new GetTermsOperation(stats.IndexName, "Name", null));
                    Assert.Equal(2, terms.Length);

                    session.Delete(companies[0]);
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    terms = store.Maintenance.Send(new GetTermsOperation(stats.IndexName, "Name", null));
                    Assert.Equal(2, terms.Length);

                    session.Delete(companies[1]);
                    session.SaveChanges();

                    Indexes.WaitForIndexing(store);

                    terms = store.Maintenance.Send(new GetTermsOperation(stats.IndexName, "Name", null));
                    Assert.Equal(1, terms.Length);
                }
            }
        }
    }
}
