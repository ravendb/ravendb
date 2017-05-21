using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_7028 : RavenTestBase
    {
        [Fact]
        public void DeleteByCollectionShouldOnlyDeleteAllDocsFromThatCollection()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company(), "companies/1");
                    session.Store(new Address(), "addresses/1");

                    session.SaveChanges();
                }

                var stats = store.Admin.Send(new GetStatisticsOperation());
                Assert.Equal(2, stats.CountOfDocuments);

                var operation = store.Operations.Send(new DeleteCollectionOperation("Companies"));

                operation.WaitForCompletion();

                stats = store.Admin.Send(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfDocuments);

                using (var session = store.OpenSession())
                {
                    var company = session.Load<Company>("companies/1");
                    Assert.Null(company);

                    var address = session.Load<Address>("addresses/1");
                    Assert.NotNull(address);
                }
            }
        }
    }
}