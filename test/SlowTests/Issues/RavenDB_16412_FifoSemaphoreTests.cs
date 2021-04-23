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

            bool tasksRunning = true;

            var verifyLimit = Task.Run(async () =>
            {
                while (tasksRunning)
                {
                    Assert.True(inUse <= allowed);

                    await Task.Delay(13);
                }
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

            var waitResult = Task.WaitAll(tasks.ToArray(), TimeSpan.FromSeconds(30));

            Assert.True(waitResult);

            tasksRunning = false;

            waitResult = verifyLimit.Wait(TimeSpan.FromSeconds(30));

            Assert.True(waitResult);

            Assert.Equal(numberOfTasks * numberOfLocksPerTask, totalNumberOfLocksTaken);
        }

        [Fact]
        public void ShouldNotLeaveWaitersInQueueIfOperationIsCancelled()
        {
            var @lock = new FifoSemaphore(1);

            @lock.Acquire(CancellationToken.None);

            var cts = new CancellationTokenSource();

            cts.Cancel();

            Assert.Throws<OperationCanceledException>(() => @lock.Acquire(cts.Token));

            Assert.Empty(@lock._waitQueue);

            cts = new CancellationTokenSource();

            @lock.ForTestingPurposesOnly().JustBeforeAddingToWaitQueue += () => cts.Cancel();

            Assert.Throws<OperationCanceledException>(() => @lock.Acquire(cts.Token));

            Assert.Empty(@lock._waitQueue);
        }

        [Fact]
        public void ShouldNotLeaveWaitersInQueueIfTimeout()
        {
            var @lock = new FifoSemaphore(1);

            @lock.Acquire(CancellationToken.None);

            Assert.False(@lock.TryAcquire(TimeSpan.Zero, CancellationToken.None));

            Assert.Empty(@lock._waitQueue);
        }
    }
}
