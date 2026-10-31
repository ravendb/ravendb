using System.Threading.Tasks;
using Sparrow.Server.Threading;
using Tests.Infrastructure;
using Xunit;

namespace FastTests.Sparrow
{
    public class PerCoreValueTests : NoDisposalNeeded
    {
        public PerCoreValueTests(ITestOutputHelper output) : base(output)
        {
        }

        [RavenFact(RavenTestCategory.Core)]
        public void SumIsTheSumOfAllTheCores()
        {
            var sum = new PerCoreValue<PerCoreSum>();

            Parallel.For(0, 64, new ParallelOptions { MaxDegreeOfParallelism = 16 }, _ =>
            {
                for (int j = 0; j < 1000; j++)
                    sum.Apply(1);
            });

            Assert.Equal(64_000, sum.Value);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void SlotAppliesToTheSameSlotFromAnotherThread()
        {
            var sum = new PerCoreValue<PerCoreSum>();

            var slots = new PerCoreValue<PerCoreSum>.Slot[64];
            Parallel.For(0, slots.Length, new ParallelOptions { MaxDegreeOfParallelism = 16 }, i => slots[i] = sum.Apply(1));
            Assert.Equal(slots.Length, sum.Value);

            // undone on other threads, so the decrement must go to the slot that was incremented, not the current one
            Parallel.For(0, slots.Length, new ParallelOptions { MaxDegreeOfParallelism = 16 }, i => slots[slots.Length - 1 - i].Apply(-1));
            Assert.Equal(0, sum.Value);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void IsZeroUntilAnySlotIsNot()
        {
            var sum = new PerCoreValue<PerCoreSum>();
            Assert.True(sum.IsZero);

            var slot = sum.Apply(1);
            Assert.False(sum.IsZero);

            slot.Apply(-1);
            Assert.True(sum.IsZero);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void MaxRaisesButDoesNotLower()
        {
            var max = new PerCoreValue<PerCoreMax>(10);
            Assert.Equal(10, max.Value);

            max.Apply(5);
            Assert.Equal(10, max.Value);

            max.Apply(20);
            Assert.Equal(20, max.Value);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void ResetCanLower()
        {
            var max = new PerCoreValue<PerCoreMax>(10);
            max.Apply(20);

            max.Reset(long.MinValue);
            Assert.Equal(long.MinValue, max.Value);

            max.Apply(3);
            Assert.Equal(3, max.Value);
        }

        [RavenFact(RavenTestCategory.Core)]
        public void MaxIsTheMaximumOfAllTheCores()
        {
            var max = new PerCoreValue<PerCoreMax>();

            Parallel.For(0, 64, new ParallelOptions { MaxDegreeOfParallelism = 16 }, i =>
            {
                for (int j = 1; j <= 1000; j++)
                    max.Apply(i * 1000 + j);
            });

            Assert.Equal(64_000, max.Value);
        }
    }
}
