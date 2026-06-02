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

                // GetPage must successfully re-decrypt the page from disk (not crash or return garbage).
                // This is the VoronStream scenario: after TryReleasePage, GetPage is used to re-fetch
                // the next page, and it must go through the pager rather than the stale locator entry.
                var refetched = llt.GetPage(pageNumber);
                Assert.True(refetched.IsValid);
                Assert.Equal(pageNumber, refetched.PageNumber);
            }
        }

        [RavenFact(RavenTestCategory.Voron | RavenTestCategory.Encryption)]
        public void Encrypted_Page_Shared_By_Two_Streams_Must_Be_Refcounted_Per_Reader()
        {
            RequireFileBasedPager();

            const int size = 40_000;
            var data = new byte[size];
            for (int i = 0; i < size; i++)
                data[i] = (byte)(i % 251);

            using (var tx = Env.WriteTransaction())
            {
                var tree = tx.CreateTree("Data");
                tree.AddStream("stream1", new MemoryStream(data));
                tx.Commit();
            }

            Env.FlushLogToDataFile();
            RestartDatabase();

            using (var tx = Env.ReadTransaction())
            {
                var llt = tx.LowLevelTransaction;
                var tree = tx.ReadTree("Data");

                // The first chunk lives on its own (overflow) page that, after flush+restart, is served
                // by CryptoPager (decrypted into a per-transaction EncryptionBuffer).
                Slice.From(tx.Allocator, "stream1", out var key);
                var chunks = tree.ReadTreeChunks(key, out _);
                Assert.True(chunks.Length >= 2, $"expected the stream to span >=2 chunks, got {chunks.Length}");
                long p0 = chunks[0].PageNumber;
                int chunk0Size = chunks[0].ChunkSize;

                // Two independent VoronStreams over the same key model two cloned Lucene index inputs
                // sharing one read transaction (hence the same crypto state and the same page locator).
                var streamA = tree.ReadStream(key);
                var streamB = tree.ReadStream(key);

                // A reads the first chunk -> page p0 is decrypted into a fresh buffer (Usages = 1) and,
                // on the regressed code path, cached in the page locator.
                Assert.NotEqual(-1, streamA.ReadByte());
                Assert.Equal(1, GetUsages(llt, p0));

                // B reads the same page. It MUST take its own reference (Usages -> 2). On the regressed
                // path VoronStream uses Llt.GetPage(), whose page-locator HIT returns the cached pointer
                // WITHOUT AcquirePagePointer, so Usages stays 1 - an un-refcounted second holder. That is
                // the root cause of the AVE: a single TryReleasePage then frees a buffer still in use.
                Assert.NotEqual(-1, streamB.ReadByte());
                Assert.Equal(2, GetUsages(llt, p0));

                // ---- use-after-free demonstration ----
                // A consumes the rest of chunk 0 (staying on p0) and then steps into chunk 1, which makes
                // VoronStream release p0 via TryReleasePage.
                var discard = new byte[chunk0Size];
                ReadExactly(streamA, discard, chunk0Size - 1);
                Assert.NotEqual(-1, streamA.ReadByte());

                // B still references p0, so the buffer must not have been freed.
                Assert.True(GetUsages(llt, p0) >= 1,
                    $"page {p0} buffer was freed while stream B still references it (use-after-free)");

                // B reads the remainder of chunk 0; against a freed/zeroed buffer this returns garbage.
                var fromB = new byte[chunk0Size - 1];
                ReadExactly(streamB, fromB, fromB.Length);
                for (int i = 0; i < fromB.Length; i++)
                    Assert.Equal((byte)((i + 1) % 251), fromB[i]);
            }

            static int GetUsages(LowLevelTransaction llt, long page)
            {
                var state = (IPagerLevelTransactionState)llt;
                if (state.CryptoPagerTransactionState == null)
                    return -1;
                foreach (var kvp in state.CryptoPagerTransactionState)
                    if (kvp.Value.TryGetValue(page, out var buffer))
                        return buffer.Usages;
                return 0; // released / never decrypted
            }

            static void ReadExactly(System.IO.Stream stream, byte[] buffer, int count)
            {
                int read = 0;
                while (read < count)
                {
                    int n = stream.Read(buffer, read, count - read);
                    Assert.True(n > 0, "unexpected end of stream");
                    read += n;
                }
            }
        }
    }
}
