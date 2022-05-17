using Tests.Infrastructure;
using System.Threading.Tasks;
using FastTests;
using SlowTests.Server.Replication;
using Xunit.Abstractions;

namespace StressTests.Server.Replication
{
    public class ExternalReplicationStressTests_NoDispose : NoDisposalNoOutputNeeded
    {
        public ExternalReplicationStressTests_NoDispose(ITestOutputHelper output) : base(output)
        {
        }

        [MultiplatformFact(RavenArchitecture.AllX64)]
        public void ExternalReplicationShouldWorkWithSmallTimeoutStress()
        {
            for (int i = 0; i < 10; i++)
            {
                Parallel.For(0, 3, RavenTestHelper.DefaultParallelOptions, _ =>
                {
                    using (var test = new ExternalReplicationTests(Output))
                    {
                        test.ExternalReplicationShouldWorkWithSmallTimeoutStress(20000).Wait(TimeSpan.FromMinutes(10));
                    }
                });
            }
        }
    }
}
