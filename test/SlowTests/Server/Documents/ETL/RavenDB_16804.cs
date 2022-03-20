using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Operations.ETL;
using Xunit;
using Xunit.Abstractions;
using User = SlowTests.Core.Utils.Entities.User;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_16804 : EtlTestBase
    {
        public RavenDB_16804(ITestOutputHelper output) : base(output)
        {
        }


        [Fact]
        public async Task CanGetBatchStopReasonFromEtlPerformanceStats()
        {
            using (var src = GetDocumentStore())
            using (var dst = GetDocumentStore())
            {
                var configuration = new RavenEtlConfiguration
                {
                    ConnectionStringName = "test",
                    Name = "aaa",
                    Transforms =
                    {
                        new Transformation
                        {
                            Name = "S1",
                            Collections = {"Users"}
                        }
                    }
                };

                AddEtl(src, configuration, new RavenConnectionString
                {
                    Name = "test",
                    TopologyDiscoveryUrls = dst.Urls,
                    Database = dst.Database,
                });

                var etlDone = WaitForEtl(src, (_, statistics) => statistics.LoadSuccesses == 10);

                using (var session = src.OpenAsyncSession())
                {
                    for (int i = 0; i < 10; i++)
                    {
                        await session.StoreAsync(new User());
                    }

                    await session.SaveChangesAsync();
                }

                Assert.True(etlDone.Wait(TimeSpan.FromSeconds(10)));

                var database = await Databases.GetDocumentDatabaseInstanceFor(src);
                var etlProcess = database.EtlLoader.Processes.First();
                var performance = etlProcess.GetPerformanceStats();

                Assert.Contains("Successfully finished loading all batch items", performance.Select(p => p.BatchStopReason));
                Assert.Contains("No more items to process", performance.Select(p => p.BatchTransformationCompleteReason));
            }
        }
    }
}
