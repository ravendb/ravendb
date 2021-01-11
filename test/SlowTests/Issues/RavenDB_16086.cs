using FastTests.Voron;
using Sparrow;
using Voron;
using Voron.Global;
using Xunit;
using Xunit.Abstractions;
namespace SlowTests.Issues
{
    public class RavenDB_16086 : StorageTest
    {
        public RavenDB_16086(ITestOutputHelper output) : base(output)
        {
        }
        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.CompressTxAboveSizeInBytes = new Size(512, SizeUnit.Kilobytes).GetValue(SizeUnit.Bytes);
        }

        [Fact]
        public unsafe void RecoveryValidationNeedsToTakeIntoAccountFreedPagesThatCouldOverlapAnotherFreedPages()
        {
            RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.DataPager.EnsureContinuous(0, 100);
                tx.LowLevelTransaction.AllocatePage(50);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var p = tx.LowLevelTransaction.AllocatePage(8, 16);
                p.Flags |= PageFlags.Overflow;
                p.OverflowSize = 7 * Constants.Storage.PageSize;
                Memory.Set(p.DataPointer, 1, 7 * Constants.Storage.PageSize);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.FreePage(16);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var p = tx.LowLevelTransaction.AllocatePage(4, 21);
                p.Flags |= PageFlags.Overflow;
                p.OverflowSize = 3 * Constants.Storage.PageSize;
                Memory.Set(p.DataPointer, 4, 1 * Constants.Storage.PageSize);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.FreePage(21);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var p = tx.LowLevelTransaction.AllocatePage(8, 24);
                p.Flags |= PageFlags.Overflow;
                p.OverflowSize = 7 * Constants.Storage.PageSize;
                Memory.Set(p.DataPointer, 2, 7 * Constants.Storage.PageSize);
                tx.Commit();
            }

            RestartDatabase();

            using (var tx = Env.WriteTransaction())
            {
            }
        }
    }
}
