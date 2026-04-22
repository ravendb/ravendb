using System.IO;
using FastTests.Voron;
using Sparrow.Platform;
using Tests.Infrastructure;
using Voron;
using Voron.Impl.Paging;
using Xunit;

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

                // After restart, pages come from the crypto buffer cache (not scratch).
                Pager cryptoPager = null;
                Pager.EncryptionBuffer buffer = null;
                long pageNumber = -1;

                for (long candidate = 0; candidate < 20; candidate++)
                {
                    Page page;
                    try { page = llt.GetPage(candidate); }
                    catch { continue; }
                    if (page.IsValid == false) continue;

                    if (llt.PagerTransactionState.ForCrypto == null) continue;

                    foreach (var kvp in llt.PagerTransactionState.ForCrypto)
                    {
                        if (kvp.Value.TryGetValue(candidate, out buffer))
                        {
                            if (buffer.Modified) continue;

                            cryptoPager = kvp.Key;
                            pageNumber = candidate;
                            break;
                        }
                    }
                    if (cryptoPager != null) break;
                }

                Assert.NotNull(cryptoPager);
                Assert.True(pageNumber >= 0);
                Assert.Equal(1, buffer.Usages);

                Assert.True(llt._pageLocator.TryGetReadOnlyPage(pageNumber, out _),
                    $"Precondition failed: page {pageNumber} should be in page locator after GetPage");

                // TryReleasePage: Usages=1->0 -> frees buffer (removes from ForCrypto, returns to pool),
                // and MUST reset the page locator entry for pageNumber.
                llt.TryReleasePage(pageNumber);

                Assert.Equal(0, buffer.Usages);
                Assert.False(
                    llt.PagerTransactionState.ForCrypto[cryptoPager].TryGetValue(pageNumber, out _),
                    $"Precondition failed: buffer for page {pageNumber} should have been freed from ForCrypto");

                // The page locator must be invalidated so the next GetPage() re-decrypts from disk
                // rather than returning a stale pointer into freed memory.
                Assert.False(llt._pageLocator.TryGetReadOnlyPage(pageNumber, out _),
                    $"After TryReleasePage freed the buffer for page {pageNumber}, " +
                    "the page locator still has a stale entry pointing to freed memory.");

                // GetPage must successfully re-decrypt the page from disk (not crash or return garbage).
                var refetched = llt.GetPage(pageNumber);
                Assert.True(refetched.IsValid);
                Assert.Equal(pageNumber, refetched.PageNumber);
            }
        }
    }
}
