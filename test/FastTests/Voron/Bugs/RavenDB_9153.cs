using Voron;
using Voron.Data.BTrees;
using Voron.Global;
using Xunit;
using Tests.Infrastructure;
using Voron.Impl.Scratch;

namespace FastTests.Voron.Bugs
{
    public class RavenDB_9153 : StorageTest
    {
        public RavenDB_9153(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ScratchMemoryBudget = new ScratchMemoryBudget(limit: 0); // the assertions are about scratch files
            base.Configure(options);
        }

        [RavenFact(RavenTestCategory.Voron)]
        public void Overflow_shrink_should_decrease_number_of_allocated_pages_in_scratch()
        {
            using (var tx = Env.WriteTransaction())
            {
                var overflowSize = 4 * Constants.Storage.PageSize;
                var page = tx.LowLevelTransaction.AllocateOverflowRawPage(overflowSize, out _, zeroPage: false);

                var allocatedPagesCount = Env.ScratchBufferPool._current.File.AllocatedPagesCount;

                var reducedOverflowSize = 2 * Constants.Storage.PageSize;

                tx.LowLevelTransaction.ShrinkOverflowPage(page.PageNumber, reducedOverflowSize, tx.LowLevelTransaction.RootObjects);

                var shrinkPages = (overflowSize - reducedOverflowSize) / Constants.Storage.PageSize;

                Assert.Equal(allocatedPagesCount - shrinkPages + 1 /* + 1 because free space handling allocated one page during shrink */,
                    Env.ScratchBufferPool._current.File.AllocatedPagesCount);
            }
        }
    }
}
