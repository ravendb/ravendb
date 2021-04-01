using System;
using System.Collections.Generic;
using System.Threading;
using System.Threading.Tasks;
using FastTests;
using Sparrow.Utils;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Issues
{
    public class RavenDB_16412_FifoSemaphoreTests : NoDisposalNeeded
    {
        public RavenDB_16412_FifoSemaphoreTests(ITestOutputHelper output) : base(output)
        {
        }

        [Fact]
        public void ShouldRespectSemaphoreLimit()
        {
            var allowed = 3;

            var inUse = 0;

            var @lock = new FifoSemaphore(allowed);

            var tasks = new List<Task>();

            int numberOfTasks = 20;

            int numberOfLocksPerTask = 10;

            var verifyLimit = Task.Run(() =>
            {
                Assert.True(inUse <= allowed);
            });

            int totalNumberOfLocksTaken = 0;

            for (int i = 0; i < numberOfTasks; i++)
            {
                var task = Task.Run(() =>
                {
                    for (int j = 0; j < numberOfLocksPerTask; j++)
                    {
                        @lock.Acquire(CancellationToken.None);

                        Interlocked.Increment(ref inUse);

                        Interlocked.Increment(ref totalNumberOfLocksTaken);

                        try
                        {
                            Thread.Sleep(j);
                        }
                        finally
                        {
                            Interlocked.Decrement(ref inUse);

                            @lock.Release();
                        }
                    }

                });

                tasks.Add(task);
            }

            Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(30));

            verifyLimit.Wait(TimeSpan.FromSeconds(30));

            Assert.Equal(numberOfTasks * numberOfLocksPerTask, totalNumberOfLocksTaken);
        }
    }
}
