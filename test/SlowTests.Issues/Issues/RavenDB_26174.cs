using System;
using System.Linq;
using System.Threading.Tasks;
using FastTests;
using Microsoft.Extensions.Primitives;
using Raven.Client.Documents.Operations.ConnectionStrings;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Handlers.Processors;
using Raven.Tests.Core.Utils.Entities;
using Tests.Infrastructure;
using Xunit;

namespace SlowTests.Issues;

public class RavenDB_26174 : RavenTestBase
{
    public RavenDB_26174(ITestOutputHelper output) : base(output)
    {
    }

    [RavenFact(RavenTestCategory.Etl)]
    public async Task LivePerformanceStatsShouldReportCompletionOfFailedBatch()
    {
        using (var store = GetDocumentStore())
        {
            var database = await GetDatabase(store.Database);

            Etl.AddEtl(store, new RavenEtlConfiguration
            {
                Name = "failing-etl",
                ConnectionStringName = "failing-etl",
                Transforms = { new Transformation { Name = "failing-script", Collections = { "Users" }, Script = "loadToUsers(this);" } }
            }, new RavenConnectionString
            {
                Name = "failing-etl",
                Database = "does-not-exist",
                TopologyDiscoveryUrls = store.Urls
            });

            await WaitForValueAsync(() => database.EtlLoader.Processes.Length, 1);

            var etls = EtlHandlerProcessorForStats.GetProcessesToReportOn(database, StringValues.Empty);

            using (var collector = new LiveEtlPerformanceCollector(database, etls))
            {
                var initialStats = await collector.Stats.TryDequeueAsync(TimeSpan.FromSeconds(30));
                Assert.True(initialStats.Item1);

                using (var session = store.OpenSession())
                {
                    session.Store(new User { Name = "Joe" }, "users/1");
                    session.SaveChanges();
                }

                var deadline = DateTime.UtcNow.AddSeconds(30);
                var reportedFailedBatch = false;

                while (reportedFailedBatch == false && DateTime.UtcNow < deadline)
                {
                    var (dequeued, stats) = await collector.Stats.TryDequeueAsync(TimeSpan.FromSeconds(1));
                    if (dequeued == false)
                        continue;

                    reportedFailedBatch = stats
                        .SelectMany(x => x.Stats)
                        .SelectMany(x => x.Performance)
                        .Any(x => x.Completed != null && x.SuccessfullyLoaded == false);
                }

                Assert.True(reportedFailedBatch);
            }
        }
    }
}
