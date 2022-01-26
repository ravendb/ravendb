using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Orders;
using Raven.Client.Documents.Session;
using Raven.Server.Config;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_15237 : RavenTestBase
    {
        public RavenDB_15237(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task CanUsePagingHint_ForCompareExchange()
        {
            using (var store = GetDocumentStore(new Options
            {
                ModifyDatabaseRecord = record => record.Settings[RavenConfiguration.GetKey(x => x.PerformanceHints.MaxNumberOfResults)] = "1"
            }))
            {
                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/1", new Company { Name = "HR" });
                    session.Advanced.ClusterTransaction.CreateCompareExchangeValue("companies/2", new Company { Name = "CF" });

                    session.SaveChanges();
                }

                using (var session = store.OpenSession(new SessionOptions { TransactionMode = TransactionMode.ClusterWide }))
                {
                    var results = session.Advanced.ClusterTransaction.GetCompareExchangeValues<Company>(new[] { "companies/1", "companies/2" });
                    Assert.Equal(2, results.Count);
                    Assert.NotNull(results["companies/1"].Value);
                    Assert.NotNull(results["companies/2"].Value);
                }

                var database = await GetDatabase(store.Database);
                var outcome = database.NotificationCenter.Paging.UpdatePagingInternal(null, out string reason);
                Assert.True(outcome, reason);

                int alertCount;
                using (database.NotificationCenter.GetStored(out var actions))
                    alertCount = actions.Count();

                Assert.Equal(1, alertCount);
            }
        }
    }
}
