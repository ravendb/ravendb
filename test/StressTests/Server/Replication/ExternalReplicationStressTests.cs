﻿using System.Threading.Tasks;
using FastTests;
using FastTests.Server.Replication;
using SlowTests.Server.Replication;
using Tests.Infrastructure;

namespace StressTests.Server.Replication
{
    public class ExternalReplicationStressTests : ReplicationTestBase
    {
        [Fact64Bit]
        public void ExternalReplicationShouldWorkWithSmallTimeoutStress()
        {
            for (int i = 0; i < 100; i++)
            {
                Parallel.For(0, 10, RavenTestHelper.DefaultParallelOptions, _ =>
                {
                    using (var test = new ExternalReplicationTests())
                    {
                        test.ExternalReplicationShouldWorkWithSmallTimeoutStress().Wait();
                    }
                });
            }
        }

        [Fact32Bit]
        public void ExternalReplicationShouldWorkWithSmallTimeoutStress32()
        {
            for (int i = 0; i < 100; i++)
            {
                Parallel.For(0, 5, RavenTestHelper.DefaultParallelOptions, _ =>
                {
                    using (var test = new ExternalReplicationTests())
                    {
                        test.ExternalReplicationShouldWorkWithSmallTimeoutStress().Wait();
                    }
                });
            }
        }
    }
}
