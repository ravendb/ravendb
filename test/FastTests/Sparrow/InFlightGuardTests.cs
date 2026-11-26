using System;
using System.Threading;
using System.Threading.Tasks;
using Sparrow.Threading;
using Sparrow.Utils;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sparrow
{
    public class InFlightGuardTests : NoDisposalNeeded
    {
        public InFlightGuardTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public void SingleSlot_RejectsAfterClose() => RejectsAfterClose(new InFlightGuard<SingleSlot>());

        [RavenFact(RavenTestCategory.Core)]
        public void PerCoreSlots_RejectsAfterClose() => RejectsAfterClose(new InFlightGuard<PerCoreSlots>());

        private static void RejectsAfterClose<TSlots>(InFlightGuard<TSlots> guard) where TSlots : struct, ISlotCount
        {
            Assert.True(guard.TryEnter(out var scope));
            scope.Dispose();

            Assert.False(guard.IsClosed);
            Assert.True(guard.TryClose());
            Assert.True(guard.IsClosed);
            Assert.False(guard.TryClose());

            Assert.False(guard.TryEnter(out _));
            Assert.Throws<LockAlreadyDisposedException>(() => guard.Enter("test"));
            Assert.True(guard.WaitForDrain(TimeSpan.Zero));
        }

        [RavenFact(RavenTestCategory.Core)]
        public void SingleSlot_WaitsForTheOperationsInFlight() => WaitsForTheOperationsInFlight(new InFlightGuard<SingleSlot>());

        [RavenFact(RavenTestCategory.Core)]
        public void PerCoreSlots_WaitsForTheOperationsInFlight() => WaitsForTheOperationsInFlight(new InFlightGuard<PerCoreSlots>());

        private static void WaitsForTheOperationsInFlight<TSlots>(InFlightGuard<TSlots> guard) where TSlots : struct, ISlotCount
        {
            Assert.True(guard.TryEnter(out var first));
            Assert.True(guard.TryEnter(out var second));

            Assert.True(guard.TryClose());
            Assert.False(guard.WaitForDrain(TimeSpan.FromMilliseconds(50)));

            first.Dispose();
            Assert.False(guard.WaitForDrain(TimeSpan.FromMilliseconds(50)));

            var exit = Task.Run(() =>
            {
                Thread.Sleep(100);
                second.Dispose();
            });

            Assert.True(guard.WaitForDrain(TimeSpan.FromSeconds(30)));
            exit.Wait();
        }

        [RavenFact(RavenTestCategory.Core)]
        public void PerCoreSlots_NoOperationEntersAfterTheCloseAndAllLeave()
        {
            var guard = new InFlightGuard<PerCoreSlots>();
            long inFlight = 0;
            long enteredAfterDrain = 0;
            var drained = false;

            var workers = new Task[Environment.ProcessorCount * 2];
            for (int i = 0; i < workers.Length; i++)
            {
                workers[i] = Task.Run(() =>
                {
                    while (true)
                    {
                        if (guard.TryEnter(out var scope) == false)
                            return;

                        // exits on another thread every now and then, the scope remembers its slot
                        Interlocked.Increment(ref inFlight);
                        if (Volatile.Read(ref drained))
                            Interlocked.Increment(ref enteredAfterDrain);
                        Interlocked.Decrement(ref inFlight);

                        if (Environment.CurrentManagedThreadId % 2 == 0)
                            Task.Run(() => scope.Dispose()).Wait();
                        else
                            scope.Dispose();
                    }
                });
            }

            Thread.Sleep(200);
            Assert.True(guard.TryClose());
            Assert.True(guard.WaitForDrain(TimeSpan.FromSeconds(30)));
            Volatile.Write(ref drained, true);

            Assert.True(Task.WaitAll(workers, TimeSpan.FromSeconds(30)));
            Assert.Equal(0, Interlocked.Read(ref inFlight));
            Assert.Equal(0, Interlocked.Read(ref enteredAfterDrain));
            Assert.False(guard.TryEnter(out _));
        }
    }
}
