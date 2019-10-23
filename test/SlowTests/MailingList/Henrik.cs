using System.Linq;
using FastTests;
using Xunit;
using Raven.Client.Documents;
using Tests.Infrastructure;
using Xunit.Abstractions;

namespace SlowTests.MailingList
{
    public class Henrik : RavenTestBase
    {
        public Henrik(ITestOutputHelper output) : base(output)
        {
        }

        private class Company
        {
            public string Id { get; set; }
            public string ExternalId { get; set; }
            public string Name { get; set; }
            public string Phone { get; set; }
            public string Fax { get; set; }
        }

        public class CompanyProjection
        {
            public string Name { get; set; }
        }

        [Fact]
        public void Different_ways_of_loading_same_projection_should_give_equivalent_results()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new CreateSampleDataOperation());

                const string companyId = "companies/89-A";

                using (var session = store.OpenSession())
                {
                    var companyProjectedFromLinq =
                        session.Query<Company>()
                            .Where(comp => comp.Id == companyId)
                            .Select(comp => new CompanyProjection { Name = comp.Name })
                            .SingleOrDefault();

                    var companyProjectionLoaded =
                        session.Load<CompanyProjection>(companyId);

                    var companyProjectedOnServer =
                        session.Query<Company>()
                            .Where(comp => comp.Id == companyId)
                            .ProjectInto<CompanyProjection>()
                            .SingleOrDefault();

                    Assert.NotNull(companyProjectedFromLinq);
                    Assert.NotNull(companyProjectionLoaded);
                    Assert.NotNull(companyProjectedOnServer);
                }
            }
        }
    }
}
