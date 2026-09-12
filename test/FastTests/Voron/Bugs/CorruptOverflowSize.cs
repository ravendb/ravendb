using System;
using System.IO;
using System.Linq;
using System.Runtime.InteropServices;
using FastTests.Voron;
using Sparrow;
using Sparrow.Platform;
using Sparrow.Utils;
using Tests.Infrastructure;
using Voron;
using Voron.Exceptions;
using Voron.Global;
using Voron.Data.Containers;
using Voron.Impl.Journal;
using Xunit;

namespace FastTests.Voron.Bugs
{
    public class CorruptOverflowSize : StorageTest
    {
        public CorruptOverflowSize(ITestOutputHelper output) : base(output)
        {
        }

        private readonly byte[] _masterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());

        [RavenTheory(RavenTestCategory.Voron)]
        [InlineData(Constants.Storage.PageSize * 1000)] // extent runs past the end of the file
        [InlineData(int.MaxValue)] // large enough that numberOfPages * PageSize wraps negative in an int multiply
        [InlineData(-1)] // negative overflow size must be rejected, it is never a valid payload length
        public void CorruptOverflowSizeOnDiskIsRejectedOnReadWithoutEncryption(int corruptOverflowSize)
        {
            CorruptOverflowSizeOnDiskIsRejectedOnRead(corruptOverflowSize, useEncryption: false);
        }

        [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Encryption)]
        [InlineData(Constants.Storage.PageSize * 1000)]
        [InlineData(int.MaxValue)]
        [InlineData(-1)]
        public void CorruptOverflowSizeOnDiskIsRejectedOnReadWithEncryption(int corruptOverflowSize)
        {
            CorruptOverflowSizeOnDiskIsRejectedOnRead(corruptOverflowSize, useEncryption: true);
        }

        private void CorruptOverflowSizeOnDiskIsRejectedOnRead(int corruptOverflowSize, bool useEncryption)
        {
            long overflowPage;

            using (var options = CreateOptions(useEncryption))
            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.WriteTransaction())
                {
                    var page = tx.LowLevelTransaction.AllocateOverflowRawPage(Constants.Storage.PageSize * 2, out _, zeroPage: false);
                    overflowPage = page.PageNumber;
                    tx.Commit();
                }

                env.FlushLogToDataFile();

                using (var operation = new WriteAheadJournal.JournalApplicator.SyncOperation(env.Journal.Applicator))
                {
                    Assert.True(operation.SyncDataFile());
                }
            }

            using (var fileStream = SafeFileStream.Create(Path.Combine(DataDir, Constants.DatabaseFilename),
                FileMode.Open,
                FileAccess.ReadWrite,
                FileShare.ReadWrite | FileShare.Delete))
            {
                fileStream.Position = overflowPage * Constants.Storage.PageSize + (long)Marshal.OffsetOf<PageHeader>(nameof(PageHeader.OverflowSize));
                var bytes = BitConverter.GetBytes(corruptOverflowSize);
                fileStream.Write(bytes, 0, bytes.Length);
            }

            using (var options = CreateOptions(useEncryption))
            using (var env = new StorageEnvironment(options))
            using (var tx = env.ReadTransaction())
            {
                Assert.Throws<VoronUnrecoverableErrorException>(() =>
                {
                    tx.LowLevelTransaction.GetPage(overflowPage);
                });
            }
        }

        [RavenTheory(RavenTestCategory.Voron | RavenTestCategory.Encryption)]
        [InlineData(true)]
        [InlineData(false)]
        public void NonOverflowPageCarryingItsOwnFieldsIsStillReadable(bool useEncryption)
        {
            // a container page keeps NumberOfOffsets and FloorOfData in the bytes an overflow page uses for OverflowSize,
            // and FloorOfData sits near the page size, so reading them as an overflow size gives a huge extent
            ContainerEntryId id;

            using (var options = CreateOptions(useEncryption))
            using (var env = new StorageEnvironment(options))
            {
                using (var tx = env.WriteTransaction())
                {
                    var containerId = Container.Create(tx.LowLevelTransaction);
                    id = Container.Allocate(tx.LowLevelTransaction, containerId, sizeof(long), out var space);
                    BitConverter.TryWriteBytes(space, 1337L);
                    tx.Commit();
                }

                using (var tx = env.ReadTransaction())
                {
                    Container.Get(tx.LowLevelTransaction, id, out var item);
                    Assert.Equal(1337L, BitConverter.ToInt64(item.ToSpan()));
                }
            }
        }

        [RavenFact(RavenTestCategory.Voron)]
        public unsafe void ValidOverflowPageIsStillReadable()
        {
            RequireFileBasedPager();

            const int overflowSize = Constants.Storage.PageSize * 2;
            var payload = Enumerable.Repeat((byte)0x42, overflowSize).ToArray();

            long overflowPage;
            using (var tx = Env.WriteTransaction())
            {
                var page = tx.LowLevelTransaction.AllocateOverflowRawPage(overflowSize, out _, zeroPage: false);
                overflowPage = page.PageNumber;
                fixed (byte* p = payload)
                    Memory.Copy(page.DataPointer, p, overflowSize);
                tx.Commit();
            }

            RestartDatabase();

            using (var tx = Env.ReadTransaction())
            {
                var page = tx.LowLevelTransaction.GetPage(overflowPage);
                Assert.True(page.IsOverflow);
                Assert.Equal(overflowSize, page.OverflowSize);
                Assert.True(new Span<byte>(page.DataPointer, overflowSize).SequenceEqual(payload));
            }
        }

        private StorageEnvironmentOptions CreateOptions(bool useEncryption)
        {
            var options = StorageEnvironmentOptions.ForPathForTests(DataDir);
            options.ManualFlushing = true;

            if (useEncryption)
                options.Encryption.MasterKey = _masterKey.ToArray();

            return options;
        }
    }
}
