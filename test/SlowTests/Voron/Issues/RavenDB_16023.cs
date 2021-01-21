using System.Linq;
using FastTests.Voron;
using Sparrow;
using Sparrow.Server;
using Voron;
using Voron.Global;
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
    }
}
