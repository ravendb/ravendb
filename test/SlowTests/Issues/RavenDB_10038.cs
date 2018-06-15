using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Tests.Core.Utils.Entities;
using Xunit;

namespace SlowTests.Issues
{
    public class RavenDB_10038 : RavenTestBase
    {
        [Fact]
        public void T1()
        {
            using (var store = GetDocumentStore())
            {
                var stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(0, stats.CountOfIdentities);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Id = "people|"
                    });

                    session.SaveChanges();
                }

                stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(1, stats.CountOfIdentities);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Id = "people|"
                    });

                    session.Store(new User
                    {
                        Id = "users|"
                    });

                    session.SaveChanges();
                }

                stats = store.Maintenance.Send(new GetStatisticsOperation());
                Assert.Equal(2, stats.CountOfIdentities);
            }
        }
    }
}
