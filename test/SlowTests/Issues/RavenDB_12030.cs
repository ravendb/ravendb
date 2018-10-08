using System.Linq;
using FastTests;
using Orders;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_12030 : RavenTestBase
    {
        [Fact]
        public void SimpleFuzzy()
        {
            using (var store = GetDocumentStore())
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Company
                    {
                        Name = "Hibernating Rhinos"
                    });

                    session.Store(new Company
                    {
                        Name = "CodeForge"
                    });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var companies = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereEquals(x => x.Name, "CoedForhe").Fuzzy(0.5m)
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.Equal("CodeForge", companies[0].Name);

                    companies = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereEquals(x => x.Name, "Hiberanting Rinhos").Fuzzy(0.5m)
                        .ToList();

                    Assert.Equal(1, companies.Count);
                    Assert.Equal("Hibernating Rhinos", companies[0].Name);

                    companies = session
                        .Advanced
                        .DocumentQuery<Company>()
                        .WhereEquals(x => x.Name, "CoedForhe").Fuzzy(0.99m)
                        .ToList();

                    Assert.Equal(0, companies.Count);
                }
            }
        }
    }
}
