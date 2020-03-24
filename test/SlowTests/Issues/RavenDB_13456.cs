using FastTests;
using Orders;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Documents.Session;
using Raven.Client.Exceptions;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_13456 : RavenTestBase
    {
        public RavenDB_13456(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CanChangeIdentityPartsSeparator()
        {
            DoNotReuseServer();

            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    var company1 = new Company();
                    session.Store(company1);

                    Assert.StartsWith("companies/1-A", company1.Id);

                    var company2 = new Company();
                    session.Store(company2);

                    Assert.StartsWith("companies/2-A", company2.Id);
                }

                using (var session = store.OpenSession())
                {
                    var company1 = new Company();
                    session.Store(company1, "companies/");

                    var company2 = new Company();
                    session.Store(company2, "companies|");

                    session.SaveChanges();

                    Assert.StartsWith("companies/000000000", company1.Id);
                    Assert.Equal("companies/1", company2.Id);
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("company:", new Company());

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("company|", new Company());

                    var e = Assert.Throws<RavenException>(() => session.SaveChanges());
                    Assert.Contains("Document id company| cannot end with '|' or '/' as part of cluster transaction", e.Message);
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("company/", new Company());

                    var e = Assert.Throws<RavenException>(() => session.SaveChanges());
                    Assert.Contains("Document id company/ cannot end with '|' or '/' as part of cluster transaction", e.Message);
                }

                store.Maintenance.Send(new PutClientConfigurationOperation(new ClientConfiguration { IdentityPartsSeparator = ':' }));

                var stats = store.Maintenance.Send(new GetStatisticsOperation()); // forcing client configuration update

                using (var session = store.OpenSession())
                {
                    var company1 = new Company();
                    session.Store(company1);

                    Assert.StartsWith("companies:3-A", company1.Id);

                    var company2 = new Company();
                    session.Store(company2);

                    Assert.StartsWith("companies:4-A", company2.Id);
                }

                using (var session = store.OpenSession())
                {
                    var company1 = new Company();
                    session.Store(company1, "companies:");

                    var company2 = new Company();
                    session.Store(company2, "companies|");

                    session.SaveChanges();

                    Assert.StartsWith("companies:000000000", company1.Id);
                    Assert.Equal("companies:2", company2.Id);
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("company:", new Company());

                    var e = Assert.Throws<RavenException>(() => session.SaveChanges());
                    Assert.Contains("Document id company: cannot end with '|' or ':' as part of cluster transaction", e.Message);
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("company|", new Company());

                    var e = Assert.Throws<RavenException>(() => session.SaveChanges());
                    Assert.Contains("Document id company| cannot end with '|' or ':' as part of cluster transaction", e.Message);
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("company/", new Company());

                    session.SaveChanges();
                }

                store.Maintenance.Send(new PutClientConfigurationOperation(new ClientConfiguration { IdentityPartsSeparator = null }));

                stats = store.Maintenance.Send(new GetStatisticsOperation()); // forcing client configuration update

                using (var session = store.OpenSession())
                {
                    var company1 = new Company();
                    session.Store(company1);

                    Assert.StartsWith("companies/5-A", company1.Id);

                    var company2 = new Company();
                    session.Store(company2);

                    Assert.StartsWith("companies/6-A", company2.Id);
                }

                using (var session = store.OpenSession())
                {
                    var company1 = new Company();
                    session.Store(company1, "companies/");

                    var company2 = new Company();
                    session.Store(company2, "companies|");

                    session.SaveChanges();

                    Assert.StartsWith("companies/000000000", company1.Id);
                    Assert.Equal("companies/3", company2.Id);
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var company = session.Advanced.ClusterTransaction.GetCompareExchangeValue<Company>("company:");
                    company.Value.Name = "HR";

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("company|", new Company());

                    var e = Assert.Throws<RavenException>(() => session.SaveChanges());
                    Assert.Contains("Document id company| cannot end with '|' or '/' as part of cluster transaction", e.Message);
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("company/", new Company());

                    var e = Assert.Throws<RavenException>(() => session.SaveChanges());
                    Assert.Contains("Document id company/ cannot end with '|' or '/' as part of cluster transaction", e.Message);
                }
            }
        }
    }
}
