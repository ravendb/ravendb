using FastTests;
using Orders;
using Raven.Client.Documents.Session;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_14006 : RavenTestBase
    {
        public RavenDB_14006(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void T1()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var company = new Company { Id = "companies/1", ExternalId = "companies/cf", Name = "CF" };
                    session.Store(company);

                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/cf", new Address { City = "Torun" });
                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var company = session.Load<Company>("companies/1", includes => includes.IncludeCompareExchangeValue(x => x.ExternalId));

                }
            }
        }
    }
}
