using System.Linq;
using FastTests.Voron;
using Sparrow;
using Sparrow.Server;
using Voron;
using Voron.Impl;
using Xunit;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_10825: StorageTest
    {
        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);

            options.MasterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());
            options.ManualFlushing = true;
        }
        
        [Fact]
        public unsafe void Encryption_buffer_of_freed_scratch_page_must_not_affect_another_overflow_allocation_on_tx_commit()
        {
            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.State.NextPageNumber = 10;
                
                var page = tx.LowLevelTransaction.AllocatePage(1, 4);

                Memory.Set(page.DataPointer, 10, 100);

                tx.LowLevelTransaction.FreePage(4);

                page = tx.LowLevelTransaction.AllocateOverflowRawPage(38888, out _, 3);
                page.DataPointer[0] = 13;

                var cryptoTxState = ((IPagerLevelTransactionState)tx.LowLevelTransaction).CryptoPagerTransactionState.Single().Value;

                // explicitly change the order of items in dictionary so we'll apply the buffer of overflow page before the already freed one
                cryptoTxState.LoadedBuffers = cryptoTxState.LoadedBuffers.Reverse().ToDictionary(x => x.Key, x => x.Value);

                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.ModifyPage(0);
                tx.Commit();
            }

            using (var tx = Env.WriteTransaction())
            {
                tx.LowLevelTransaction.ModifyPage(0);
                tx.Commit();
            }

            Env.FlushLogToDataFile();

            using (var tx = Env.ReadTransaction())
            {
                // ensure we can decrypt it
                var overflow = tx.LowLevelTransaction.GetPage(3);

                Assert.Equal(3, overflow.PageNumber);
                Assert.Equal(38888, overflow.OverflowSize);
                Assert.Equal(13, overflow.DataPointer[0]);
            }
        }
    }
}
