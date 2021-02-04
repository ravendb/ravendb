using System.Linq;
using FastTests.Voron;
using Sparrow;
using Sparrow.Server;
using Voron;
using Voron.Data;
using Voron.Global;
using Voron.Impl;
using Voron.Impl.Journal;
using Voron.Impl.Paging;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_16023 : StorageTest
    {
        public RavenDB_16023(ITestOutputHelper output) : base(output)
        {
        }

        private readonly byte[] _masterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.Encryption.MasterKey = _masterKey.ToArray();
        }

        [Fact]
        public unsafe void RecoveryOfEncryptedStorageNeedsToTakeIntoAccountFreedOverflowPages()
        {
            RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.DataPager.EnsureContinuous(20, 10);

                tx.LowLevelTransaction.AllocatePage(50);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var p = tx.LowLevelTransaction.AllocatePage(10, 21);

                p.Flags |= PageFlags.Overflow;
                p.OverflowSize = 9 * Constants.Storage.PageSize;

                Memory.Set(p.DataPointer, 1, 9 * Constants.Storage.PageSize);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.FreePage(21);

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

            using (var tx = Env.ReadTransaction())
            {
                Page page = tx.LowLevelTransaction.GetPage(21); // must not throw 'Unable to decrypt page'
            }
        }

        [Fact]
        public unsafe void RecoveryOfEncryptedStorageNeedsToTakeIntoAccountFreedPagesThatCouldOverlapAnotherFreedPages()
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
                var p = tx.LowLevelTransaction.AllocatePage(10, 24);

                p.Flags |= PageFlags.Overflow;
                p.OverflowSize = 9 * Constants.Storage.PageSize;

                Memory.Set(p.DataPointer, 1, 9 * Constants.Storage.PageSize);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.FreePage(24);

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
                Page page = tx.LowLevelTransaction.GetPage(24); // must not throw 'Unable to decrypt page'
            }
        }

        [Fact]
        public unsafe void FreeingPageShouldAlsoMarkItsRelatedEncryptionBufferAsNotValidForCommit()
        {
            RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.DataPager.EnsureContinuous(20, 10);

                tx.LowLevelTransaction.AllocatePage(50);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                var p = tx.LowLevelTransaction.AllocatePage(9, 21);

                p.Flags |= PageFlags.Overflow;
                p.OverflowSize = 8 * Constants.Storage.PageSize;

                Memory.Set(p.DataPointer, 1, 8 * Constants.Storage.PageSize);

                tx.LowLevelTransaction.FreePage(21);

                p = tx.LowLevelTransaction.AllocatePage(10, 20);

                p.Flags |= PageFlags.Overflow;
                p.OverflowSize = 9 * Constants.Storage.PageSize;

                Memory.Set(p.DataPointer, 1, 9 * Constants.Storage.PageSize);

                tx.LowLevelTransaction.FreePage(20);

                p = tx.LowLevelTransaction.AllocatePage(2, 21);

                p.Flags |= PageFlags.Overflow;
                p.OverflowSize =  Constants.Storage.PageSize;

                Memory.Set(p.DataPointer, 2, Constants.Storage.PageSize);

                var cryptoPagerTransactionState = ((IPagerLevelTransactionState)tx.LowLevelTransaction).CryptoPagerTransactionState;

                var scratchFile = Env.ScratchBufferPool.GetScratchBufferFile(0);

                var state = cryptoPagerTransactionState[scratchFile.File.Pager];

                Assert.True(state[66].SkipOnTxCommit); // page 66 is PositionInScratchBuffer of the age #21 that was freed at the beginning of this transaction
                Assert.True(state[83].SkipOnTxCommit); // page 83 is PositionInScratchBuffer of the age #20 that was freed in this transaction

                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                Page page = tx.LowLevelTransaction.GetPage(21);

                for (int i = 0; i < Constants.Storage.PageSize; i++)
                {
                    Assert.Equal(2, page.DataPointer[i]);
                }
            }
        }
    }
}
