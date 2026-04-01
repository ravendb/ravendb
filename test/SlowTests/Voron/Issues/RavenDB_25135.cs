using System.IO;
using FastTests.Voron;
using Sparrow.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Impl;
using Voron.Impl.Paging;
using Xunit;
using Xunit.Abstractions;

namespace SlowTests.Voron.Issues
{
    public class RavenDB_25135 : StorageTest
    {
        private readonly byte[] _masterKey = Sodium.GenerateRandomBuffer((int)Sodium.crypto_aead_xchacha20poly1305_ietf_keybytes());

        public RavenDB_25135(ITestOutputHelper output) : base(output)
        {
        }

        protected override void Configure(StorageEnvironmentOptions options)
        {
            base.Configure(options);
            options.Encryption.MasterKey = (byte[])_masterKey.Clone();
            options.ManualFlushing = true;
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Encryption)]
        public void Page_Locator_Must_Be_Invalidated_After_TryReleasePage_Frees_Buffer()
        {
            RequireFileBasedPager();

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Data");
                tree.Add("key1", new MemoryStream(new byte[100]));
                tx.Commit();
            }

            Env.FlushLogToDataFile();
            RestartDatabase();

            using (var tx = Env.ReadTransaction())
            {
                var llt = tx.LowLevelTransaction;
                var pagerTxState = (IPagerLevelTransactionState)llt;

                // After restart, pages come from CryptoPager (not journal)
                CryptoPager cryptoPager = null;
                EncryptionBuffer buffer = null;
                long pageNumber = -1;

                for (long candidate = 0; candidate < 20; candidate++)
                {
                    Page page;
                    try { page = llt.GetPage(candidate); }
                    catch { continue; }
                    if (page.IsValid == false) continue;

                    if (pagerTxState.CryptoPagerTransactionState == null) continue;

                    foreach (var kvp in pagerTxState.CryptoPagerTransactionState)
                    {
                        if (kvp.Value.TryGetValue(candidate, out buffer))
                        {
                            // Only pick buffers that can be returned (not sub-pages of overflow allocations)
                            if (CryptoPager.CanReturnBuffer(buffer) == false) continue;
                            if (buffer.Modified) continue;

                            cryptoPager = (CryptoPager)kvp.Key;
                            pageNumber = candidate;
                            break;
                        }
                    }
                    if (cryptoPager != null) break;
                }

                // After restart, data pages MUST be served through CryptoPager
                Assert.NotNull(cryptoPager);
                Assert.True(pageNumber >= 0);
                Assert.Equal(1, buffer.Usages);

                // PageLocator.SetReadable has a guard: it only updates the cache entry if the
                // bucket has a DIFFERENT page number. If the same page number is already at that
                // bucket (even with a stale generation), SetReadable is a no-op.
                // Workaround: Reset the bucket first, then GetPage to get a fresh locator entry.
                llt._pageLocator.Reset(pageNumber);
                _ = llt.GetPage(pageNumber); // AcquirePagePointer again -> Usages=2, locator freshly set

                // Precondition: locator must now have a fresh entry for pageNumber
                Assert.True(llt._pageLocator.TryGetReadOnlyPage(pageNumber, out _),
                    $"Precondition failed: page {pageNumber} should be in page locator after Reset+GetPage");

                Assert.Equal(2, buffer.Usages);

                // First TryReleasePage: Usages=2->1, CanRelease=false -> buffer stays, locator stays
                cryptoPager.TryReleasePage(pagerTxState, pageNumber);
                Assert.Equal(1, buffer.Usages);

                // Second TryReleasePage: Usages=1->0 -> frees buffer (sodium_memzero + ReturnBuffer),
                // removes from CryptoTransactionState, and resets the page locator entry.
                cryptoPager.TryReleasePage(pagerTxState, pageNumber);
                Assert.Equal(0, buffer.Usages);

                // The page locator must be invalidated so the next GetPage() re-decrypts from disk
                // rather than returning a stale pointer into zeroed/freed memory.
                Assert.False(llt._pageLocator.TryGetReadOnlyPage(pageNumber, out _),
                    $"After CryptoPager.TryReleasePage freed the buffer for page {pageNumber}, " +
                    "the page locator still has a stale entry pointing to zeroed/freed memory.");
            }
        }
    }
}
