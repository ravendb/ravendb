using System.Linq;
using FastTests.Voron;
using Sparrow;
using Sparrow.Binary;
using Sparrow.Platform;
using Voron;
using Voron.Global;
using Voron.Impl;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_16105 : StorageTest
    {
        public RavenDB_16105(ITestOutputHelper output) : base(output)
        {
        }

        private readonly byte[] _masterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());

        protected override void Configure(StorageEnvironmentOptions options)
        {
            options.ManualFlushing = true;
            options.Encryption.MasterKey = _masterKey.ToArray();
            options.Encryption.EncryptionBuffersPool = new EncryptionBuffersPool();
        }

        [Fact]
        public unsafe void MustNotEncryptBuffersThatWereExtraAllocatedJustToSatisfyPowerOf2Size()
        {
            RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.AllocatePage(50);

                tx.Commit();
            }

            var numberOfAllocatedPages = 58;
            int dataSizeOnSinglePage = Constants.Storage.PageSize - PageHeader.SizeOf;
            long pageNumber, overflowPageNumber;
            using (var tx = Env.WriteTransaction())
            {
                // this will allocate encryption buffer of size 64 pages because we use Bits.PowerOf2(numberOfPages) under the covers
                // in the scratch file the allocation will start at position 66
                var p = tx.LowLevelTransaction.AllocateMultiplePageAndReturnFirst(numberOfAllocatedPages);
                pageNumber = p.PageNumber;

                for (int i = 0; i < numberOfAllocatedPages; i++)
                {
                    Page page = tx.LowLevelTransaction.GetPage(pageNumber + i);

                    Memory.Set(page.DataPointer, (byte)i, dataSizeOnSinglePage);
                }

                var scratchFile = Env.ScratchBufferPool.GetScratchBufferFile(0);

                var state = tx.LowLevelTransaction.PagerTransactionState.ForCrypto![scratchFile.Pager];
                long positionInScratchBuffer = Env.WriteTransactionPool.ScratchPagesInUse[p.PageNumber].PositionInScratchBuffer;
                // starting position 66 in the scratch file + 58 pages of actual allocation
                for (int i = numberOfAllocatedPages; i < Bits.PowerOf2(numberOfAllocatedPages); i++)
                {
                    Assert.False(state.TryGetValue(positionInScratchBuffer + i, out _));
                }
                
                for (int i = 0; i < numberOfAllocatedPages; i++)
                {
                    Assert.True(state[66 + i].Modified); // pages in use must have Modified = true
                }

                Page overFlow = tx.LowLevelTransaction.AllocatePage(9);
                overFlow.Flags |= PageFlags.Overflow;
                overFlow.OverflowSize = 8 * Constants.Storage.PageSize;
                overflowPageNumber = overFlow.PageNumber;
                Memory.Set(overFlow.DataPointer, (byte)17, overFlow.OverflowSize);

                
                
                tx.Commit();
            }

            using (var tx = Env.ReadTransaction())
            {
                Page overflowPage = tx.LowLevelTransaction.GetPage(overflowPageNumber);
                Assert.Equal(8*Constants.Storage.PageSize, overflowPage.OverflowSize);
                for (int i = 0; i < overflowPage.OverflowSize; i++)
                {
                    Assert.Equal(17, overflowPage.DataPointer[i]);
                }
                for (int i = 0; i < numberOfAllocatedPages; i++)
                {
                    Page page = tx.LowLevelTransaction.GetPage(pageNumber + i);

                    for (int j = 0; j < dataSizeOnSinglePage; j++)
                    {
                        Assert.Equal(i, page.DataPointer[j]);
                    }
                }
            }
        }
    }
}
