using FastTests.Voron;
using Sparrow;
using Voron;
using Voron.Global;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_12524 : StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
        }

        [Fact]
        public void RecoveryValidationNeedsToTakeIntoAccountOverflows()
        {
            RequireFileBasedPager();

            long pageNum;

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.DataPager.EnsureContinuous(20, 10);

                var page = tx.LowLevelTransaction.AllocatePage(50);

                pageNum = page.PageNumber;

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.FreePage(pageNum);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.AllocatePage(1, 21);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.FreePage(21);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.AllocatePage(2, 20);

                tx.Commit();
            }

            RestartDatabase();

            using (var tx = Env.WriteTransaction())
            {

            }
        }

        [Fact]
        public unsafe void RecoveryValidationNeedsToTakeIntoAccountFreedPages()
        {
            RequireFileBasedPager();

            long pageNum;

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.DataPager.EnsureContinuous(20, 10);

                var page = tx.LowLevelTransaction.AllocatePage(50);

                pageNum = page.PageNumber;

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.FreePage(pageNum);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var p = tx.LowLevelTransaction.AllocatePage(10, 20);

                p.Flags |= PageFlags.Overflow;
                p.OverflowSize = 9 * Constants.Storage.PageSize;

                Memory.Set(p.DataPointer, 1, 9 * Constants.Storage.PageSize);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.FreePage(20);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var p = tx.LowLevelTransaction.AllocatePage(2, 21);

                Memory.Set(p.DataPointer, 2, Constants.Storage.PageSize);

                tx.Commit();
            }

            RestartDatabase();

            using (var tx = Env.WriteTransaction())
            {

            }
        }
    }
}
