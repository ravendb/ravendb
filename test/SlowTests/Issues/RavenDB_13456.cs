using FastTests;
using Orders;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.Configuration;
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
            }
        }
    }
}
