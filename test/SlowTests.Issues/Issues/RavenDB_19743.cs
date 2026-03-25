using FastTests;
using Orders;
using Raven.Client.Documents.Operations.Configuration;
using Raven.Client.Exceptions.Documents.Session;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_19743 : RavenTestBase
    {
        public RavenDB_19743(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.ClientApi)]
        public void DisabledConfigurationBaseCase()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutClientConfigurationOperation(new ClientConfiguration
                {
                    IdentityPartsSeparator = '!',
                    Disabled = true
                }));
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new Company
                        {
                            Id = "company|"
                        });
                        session.SaveChanges();
                    }
                    var company = session.Advanced.LoadStartingWith<Company>("company");

                    for (int i = 0; i < 10; i++)
                        Assert.StartsWith("company/", company[i].Id);
                }
            }
        }

        [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.ClientApi)]
        public void DisabledConfigurationGoodCase()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutClientConfigurationOperation(new ClientConfiguration
                {
                    IdentityPartsSeparator = '!',
                    Disabled = false
                }));
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new Company
                        {
                            Id = "company|"
                        });

                        session.SaveChanges();
                    }
                    var company = session.Advanced.LoadStartingWith<Company>("company");

                    for (int i = 0; i < 10; i++)
                        Assert.StartsWith("company!", company[i].Id);
                }
            }
        }

        [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.ClientApi)]
        public void DisabledConfigurationDefaultChange_tryToCrateDataWithDisabledSigh_shouldThrow()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutClientConfigurationOperation(new ClientConfiguration
                {
                    IdentityPartsSeparator = '!',
                    Disabled = true
                }));

                Assert.Throws<NonUniqueObjectException>(() =>
                {
                    using (var session = store.OpenSession())
                    {
                        for (int i = 0; i < 10; i++)
                        {
                            session.Store(new Company { Id = "companies!" });

                            session.SaveChanges();
                        }
                    }
                });
            }
        }

        [RavenFact(RavenTestCategory.Configuration | RavenTestCategory.ClientApi)]
        public void CrateValidConfigurationThenDisable_ShouldCrateDataWithDefaultPartSeparator()
        {
            using (var store = GetDocumentStore())
            {
                store.Maintenance.Send(new PutClientConfigurationOperation(new ClientConfiguration
                {
                    IdentityPartsSeparator = '!',
                    Disabled = false
                }));
                store.Maintenance.Send(new PutClientConfigurationOperation(new ClientConfiguration
                {
                    Disabled = true
                }));
                using (var session = store.OpenSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        session.Store(new Company
                        {
                            Id = "company|"
                        });
                        session.SaveChanges();
                    }
                    var company = session.Advanced.LoadStartingWith<Company>("company");

                    for (int i = 0; i < company.Length; i++)
                        Assert.StartsWith("company/", company[i].Id);
                }
            }
        }
    }
}
