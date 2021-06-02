using System;
using System.Linq;
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
        public void Processing_in_low_memory_mode()
        {
            using (var src = GetDocumentStore())
            using (var dest = GetDocumentStore())
            {
                AddEtl(src, dest, "Users", script: null);

                var database = GetDatabase(src.Database).Result;

                var etlProcess = (RavenEtl)database.EtlLoader.Processes.First();

                etlProcess.LowMemory(LowMemorySeverity.ExtremelyLow);

                var numberOfDocs = EtlProcess<RavenEtlItem, ICommandData, RavenEtlConfiguration, RavenConnectionString, EtlStatsScope, EtlPerformanceOperation>.MinBatchSize + 50;

                var etlDone = WaitForEtl(src, (n, s) => s.LoadSuccesses >= numberOfDocs);

                using (var session = src.OpenSession())
                {
                    for (int i = 0; i < numberOfDocs; i++)
                    {
                        session.Store(new User()
                        {
                            Name = "Joe Doe"
                        }, $"users/{i}");
                    }

                    session.SaveChanges();
                }

                etlDone.Wait(TimeSpan.FromMinutes(1));

                for (int i = 0; i < numberOfDocs; i++)
                {
                    using (var session = dest.OpenSession())
                    {
                        var user = session.Load<User>($"users/{i}");

                        Assert.NotNull(user);
                    }
                }

                var stats = etlProcess.GetPerformanceStats();

                Assert.Contains("The batch was stopped after processing 64 items because of low memory", stats.Select(x => x.BatchCompleteReason));
            }
        }
    }
}
