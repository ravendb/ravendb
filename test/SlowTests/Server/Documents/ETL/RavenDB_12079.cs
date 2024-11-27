using System;
using System.Linq;
using System.Threading.Tasks;
using Raven.Client.Documents.Commands.Batches;
using Raven.Client.Documents.Operations.ETL;
using Raven.Server.Documents.ETL;
using Raven.Server.Documents.ETL.Providers.Raven;
using Raven.Server.Documents.ETL.Stats;
using Raven.Tests.Core.Utils.Entities;
using Sparrow.LowMemory;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Server.Documents.ETL
{
    public class RavenDB_12079 : EtlTestBase
    {
        public RavenDB_12079(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public async Task Processing_in_low_memory_mode()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script: null);

                var database = await GetDatabase(src.Database);

                var etlProcess = (RavenEtl)database.EtlLoader.Processes.First();

                etlProcess.LowMemory(LowMemorySeverity.ExtremelyLow);

                var numberOfDocs = EtlProcess<RavenEtlItem, ICommandData, RavenEtlConfiguration, RavenConnectionString, EtlStatsScope, EtlPerformanceOperation>.MinBatchSize + 50;

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses >= numberOfDocs);

                using (var session = src.OpenAsyncSession())
                {
                    for (int i = 0; i < numberOfDocs; i++)
                    {
                        await session.StoreAsync(new User
                        {
                            Name = "Joe Doe"
                        }, $"users/{i}");
                    }

                    await session.SaveChangesAsync();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                for (int i = 0; i < numberOfDocs; i++)
                {
                    using (var session = dest.OpenAsyncSession())
                    {
                        var user = await session.LoadAsync<User>($"users/{i}");

                        Assert.NotNull(user);
                    }
                }

                var stats = etlProcess.GetPerformanceStats();

                Assert.Contains("The batch was stopped after processing 64 items because of low memory", stats.Select(x => x.BatchTransformationCompleteReason));
            }
        }
    }
}
