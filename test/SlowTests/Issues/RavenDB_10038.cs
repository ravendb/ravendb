using FastTests;
using Raven.Client.Documents.Operations;
using Raven.Client.Documents.Operations.CompareExchange;
using Raven.Tests.Core.Utils.Entities;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_10038 : RavenTestBase
    {
        public RavenDB_10038(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void CompareExchangeAndIdentitiesCount()
        {
            using (var store = GetDocumentStore())
            {
                var stats = store.Maintenance.Send(new GetDetailedStatisticsOperation());
                Assert.Equal(0, stats.CountOfIdentities);
                Assert.Equal(0, stats.CountOfCompareExchange);

                using (var session = store.OpenSession())
                {
                    session.Store(new Person
                    {
                        Id = "people|"
                    });

                    session.SaveChanges();
                }

                stats = store.Maintenance.Send(new GetDetailedStatisticsOperation());
                Assert.Equal(1, stats.CountOfIdentities);
                Assert.Equal(0, stats.CountOfCompareExchange);

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

                stats = store.Maintenance.Send(new GetDetailedStatisticsOperation());
                Assert.Equal(2, stats.CountOfIdentities);
                Assert.Equal(0, stats.CountOfCompareExchange);

                store.Operations.Send(new PutCompareExchangeValueOperation<Person>("key/1", new Person(), 0));

                stats = store.Maintenance.Send(new GetDetailedStatisticsOperation());
                Assert.Equal(2, stats.CountOfIdentities);
                Assert.Equal(1, stats.CountOfCompareExchange);

                var result = store.Operations.Send(new PutCompareExchangeValueOperation<Person>("key/2", new Person(), 0));
                Assert.True(result.Successful);

                stats = store.Maintenance.Send(new GetDetailedStatisticsOperation());
                Assert.Equal(2, stats.CountOfIdentities);
                Assert.Equal(2, stats.CountOfCompareExchange);

                result = store.Operations.Send(new PutCompareExchangeValueOperation<Person>("key/2", new Person(), result.Index));
                Assert.True(result.Successful);

                stats = store.Maintenance.Send(new GetDetailedStatisticsOperation());
                Assert.Equal(2, stats.CountOfIdentities);
                Assert.Equal(2, stats.CountOfCompareExchange);

                result = store.Operations.Send(new DeleteCompareExchangeValueOperation<Person>("key/2", result.Index));
                Assert.True(result.Successful);

                stats = store.Maintenance.Send(new GetDetailedStatisticsOperation());
                Assert.Equal(2, stats.CountOfIdentities);
                Assert.Equal(1, stats.CountOfCompareExchange);
            }
        }
    }
}
