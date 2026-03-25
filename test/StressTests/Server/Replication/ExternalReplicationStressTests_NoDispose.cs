using System;
using System.Threading.Tasks;
using FastTests;
using SlowTests.Server.Replication;
using Tests.Infrastructure;
using Xunit;

namespace StressTests.Server.Replication
{
    public class ExternalReplicationStressTests_NoDispose : NoDisposalNoOutputNeeded
    {
        public ExternalReplicationStressTests_NoDispose(ITestOutputHelper output) : base(output)
        {
        }

        [RavenMultiplatformTheory(RavenTestCategory.Replication, RavenArchitecture.AllX64)]
        [RavenData(DatabaseMode = RavenDatabaseMode.All)]
        public async Task ExternalReplicationShouldWorkWithSmallTimeoutStress(RavenTestBase.Options options)
        {
            for (int i = 0; i < 10; i++)
            {
                await Parallel.ForAsync(0, 3, RavenTestHelper.DefaultParallelOptions, async (_, _) =>
                {
                    await using var test = new ExternalReplicationTests(Output);
                    await test.ExternalReplicationShouldWorkWithSmallTimeoutStress(options, 20000);
                });
            }
        }
    }
}
