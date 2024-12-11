using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Tests.Infrastructure.Entities;
using Xunit;
using Xunit.Abstractions;

namespace FastTests.Issues
{
    public class RavenDB_23261 : RavenTestBase
    {
        public RavenDB_23261(ITestOutputHelper output) : base(output)
        {
        }

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Querying)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public void CanRunCustomRqlQueryWithLoadAndSelect(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order { Id = "orders/830-A", Employee = "John Doe", Company = "companies/1-A" }, "orders/830-A");

                    session.Store(new Company { Name = "Acme Inc." }, "companies/1-A");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                    var query = session.Advanced.RawQuery<dynamic>(@"
                        from 'Orders' as o
                       where id() == 'orders/830-A'
                       load o.Company as c
                       select {
                        Handled: o.Employee,
                        CompName: c.Name
                    }");

                    var result = query.FirstOrDefault();

                    // Assert
                    Assert.NotNull(result);
                    Assert.Equal("John Doe", result.Handled.ToString());
                }
            }
        }

        [RavenTheory(RavenTestCategory.ClientApi | RavenTestCategory.Querying | RavenTestCategory.Sharding)]
        [RavenData(DatabaseMode = RavenDatabaseMode.Sharded)]
        public void CanRunCustomRqlQueryWithLoadAndSelectSameShard(Options options)
        {
            using (var store = GetDocumentStore(options))
            {
                using (var session = store.OpenSession())
                {
                    session.Store(new Order
                    {
                        Id = "orders/830-A",
                        Employee = "John Doe",
                        Company = "companies/1-A$orders/830-A"
                    }, "orders/830-A");

                    session.Store(new Company { Name = "Acme Inc." }, "companies/1-A$orders/830-A");

                    session.SaveChanges();
                }

                using (var session = store.OpenSession())
                {
                        var query = session.Advanced.RawQuery<dynamic>(@"
                        from 'Orders' as o
                       where id() == 'orders/830-A'
                       load o.Company as c
                       select {
                        Handled: o.Employee,
                        CompName: c.Name
                    }");

                    var result = query.FirstOrDefault();

                    // Assert
                    Assert.NotNull(result);
                    Assert.Equal("John Doe", result.Handled.ToString());
                    Assert.Equal("Acme Inc.", result.CompName.ToString());
                }
            }
        }
    }
}
